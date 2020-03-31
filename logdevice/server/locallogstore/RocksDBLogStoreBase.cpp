/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/RocksDBLogStoreBase.h"

#include <rocksdb/iostats_context.h>

#include "logdevice/common/stats/PerShardHistograms.h"
#include "logdevice/server/locallogstore/RocksDBCustomiser.h"
#include "logdevice/server/locallogstore/RocksDBMemTableRep.h"
#include "logdevice/server/locallogstore/RocksDBSettings.h"
#include "logdevice/server/locallogstore/RocksDBWriter.h"

namespace facebook { namespace logdevice {

using RocksDBKeyFormat::LogSnapshotBlobKey;

const char* const RocksDBLogStoreBase::OLD_SCHEMA_VERSION_KEY =
    "schema_version";
const char* const RocksDBLogStoreBase::NEW_SCHEMA_VERSION_KEY =
    ".schema_version";

RocksDBLogStoreBase::RocksDBLogStoreBase(uint32_t shard_idx,
                                         uint32_t num_shards,
                                         const std::string& path,
                                         RocksDBLogStoreConfig rocksdb_config,
                                         RocksDBCustomiser* customiser,
                                         StatsHolder* stats_holder,
                                         IOTracing* io_tracing)
    : shard_idx_(shard_idx),
      num_shards_(num_shards),
      db_path_(path),
      customiser_(customiser),
      is_db_local_(customiser_->isDBLocal()),
      writer_(new RocksDBWriter(this, *rocksdb_config.getRocksDBSettings())),
      stats_(stats_holder),
      statistics_(rocksdb_config.options_.statistics),
      rocksdb_config_(std::move(rocksdb_config)) {
  io_tracing_ = io_tracing;
  // Per RocksDB instance option overrides.
  installMemTableRep();
}

RocksDBLogStoreBase::~RocksDBLogStoreBase() {
  if (fail_safe_mode_.load()) {
    PER_SHARD_STAT_DECR(getStatsHolder(), failed_safe_log_stores, shard_idx_);
  }

  // Clears the last reference to all column family handles in the map
  // by copying it to a vector and then clearing it. This is required to
  // satisfy TSAN which otherwise will complain about lock-order-inversion
  // There are two locks that are acquired
  // 1/ cf_accessor_ 's lock
  // 2/ RocksDB internal lock when flush is called
  // Destructor thread T1 acquires 1 followed by 2 (because destroying cf
  // calls flush)
  // Other flush thread T2 can acquire 2 followed by 1 (as part of callback to
  // markMemtableRepImmutable)
  // By moving the handles out of map and then destroying, we are preventing
  // destructor thread from acquiring 2 while holding 1
  std::vector<RocksDBCFPtr> cf_to_delete;
  cf_accessor_.withWLock([&](auto& locked_accessor) {
    for (auto& kv : locked_accessor) {
      cf_to_delete.push_back(std::move(kv.second));
      kv.second.reset();
    }
  });
  cf_to_delete.clear();

  // Destruction of db_ could trigger a flush of dirty memtable
  // when WAL is not used for writes. Such a flush, could in turn
  // callback into this class if we have regiestered event listeners.
  // Hence we should not depend on the default order of destruction
  // but rather destroy here so that callback does not get called on
  // a semi-destroyed object
  db_.reset();
}

RocksDBIterator
RocksDBLogStoreBase::newIterator(rocksdb::ReadOptions ropt,
                                 rocksdb::ColumnFamilyHandle* cf) const {
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ropt, cf));
  ld_check(it != nullptr);
  return RocksDBIterator(std::move(it), ropt, this);
}

int RocksDBLogStoreBase::sync(Durability durability) {
  ld_check(!getSettings()->read_only);
  if (durability <= Durability::ASYNC_WRITE && syncWAL() != 0) {
    return -1;
  }
  if (durability <= Durability::MEMORY) {
    return flushAllMemtables();
  }
  return 0;
}

int RocksDBLogStoreBase::flushAllMemtables(bool wait) {
  // Assume default column family only.
  auto options = rocksdb::FlushOptions();
  options.wait = wait;
  rocksdb::Status status = db_->Flush(options);
  if (!status.ok()) {
    enterFailSafeIfFailed(status, "Flush()");
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }
  return 0;
}

int RocksDBLogStoreBase::isCFEmpty(rocksdb::ColumnFamilyHandle* cf) const {
  RocksDBIterator it = newIterator(getDefaultReadOptions(), cf);
  it.Seek(rocksdb::Slice("", 0));
  // schema_version isn't visible from outside of this LocalLogStore class,
  // so it doesn't count as non-emptiness.
  if (it.status().ok() && it.Valid() &&
      (it.key().compare(OLD_SCHEMA_VERSION_KEY) == 0 ||
       it.key().compare(NEW_SCHEMA_VERSION_KEY) == 0)) {
    it.Next();
  }
  if (!it.status().ok()) {
    ld_error("Error checking if database is empty: %s",
             it.status().ToString().c_str());
    return -1;
  }
  return !it.Valid();
}

void RocksDBLogStoreBase::installMemTableRep() {
  auto create_memtable_factory = [this]() {
    mtr_factory_ = std::make_shared<RocksDBMemTableRepFactory>(
        this,
        std::make_unique<rocksdb::SkipListFactory>(
            getSettings()->skip_list_lookahead));
  };

  if (!rocksdb_config_.options_.memtable_factory) {
    create_memtable_factory();
  } else {
    // In tests someone might want to override the memtable factory
    // implementation. Allowing to do that.
    mtr_factory_ = std::dynamic_pointer_cast<RocksDBMemTableRepFactory>(
        rocksdb_config_.options_.memtable_factory);
    if (!mtr_factory_) {
      create_memtable_factory();
    } else {
      mtr_factory_->setStore(this);
    }
  }

  rocksdb_config_.options_.memtable_factory =
      rocksdb_config_.metadata_options_.memtable_factory = mtr_factory_;
}

FlushToken RocksDBLogStoreBase::maxFlushToken() const {
  return mtr_factory_->maxFlushToken();
}

FlushToken RocksDBLogStoreBase::flushedUpThrough() const {
  return mtr_factory_->flushedUpThrough();
}

SteadyTimestamp RocksDBLogStoreBase::oldestUnflushedDataTimestamp() const {
  return mtr_factory_->oldestUnflushedDataTimestamp();
}

void RocksDBLogStoreBase::throttleIOIfNeeded(WriteBufStats buf_stats,
                                             uint64_t memory_limit) {
  auto state_to_str = [](LocalLogStore::WriteThrottleState st) {
    switch (st) {
      case LocalLogStore::WriteThrottleState::NONE:
        return "NONE";
      case LocalLogStore::WriteThrottleState::STALL_LOW_PRI_WRITE:
        return "STALL_LOW_PRI_WRITE";
      case LocalLogStore::WriteThrottleState::REJECT_WRITE:
        return "REJECT_WRITE";
    }
    ld_check(false);
    return "invalid";
  };

  auto new_state = WriteThrottleState::NONE;

  if (rocksdb_config_.use_ld_managed_flushes_) {
    // Logic that throttles write IO if memory consumption is beyond limits.
    if (buf_stats.active_memory_usage + buf_stats.memory_being_flushed >=
        memory_limit / 2) {
      // Check if active memory threshold is above write stall threshold.
      new_state = buf_stats.active_memory_usage > memory_limit / 2 *
                  getSettings()->low_pri_write_stall_threshold_percent / 100
          ? LocalLogStore::WriteThrottleState::STALL_LOW_PRI_WRITE
          : LocalLogStore::WriteThrottleState::NONE;

      // If sum of active memory usage and amount of memory being flushed goes
      // above two times per shard limit, start rejecting writes. This will also
      // stall low priority writes.
      if (buf_stats.active_memory_usage + buf_stats.memory_being_flushed >=
          memory_limit) {
        new_state = WriteThrottleState::REJECT_WRITE;

        if (buf_stats.active_memory_usage + buf_stats.memory_being_flushed >=
            memory_limit * 1.5) {
          RATELIMIT_WARNING(
              std::chrono::seconds(1),
              1,
              "Shard %d active+flushing memtable size is far above the limit: "
              "%.3f MB (%.3f MB active + %.3f MB flushing) > %.3f MB. Write "
              "throttling is supposed to prevent that, please investigate.",
              static_cast<int>(getShardIdx()),
              (buf_stats.active_memory_usage + buf_stats.memory_being_flushed) /
                  1e6,
              buf_stats.active_memory_usage / 1e6,
              buf_stats.memory_being_flushed / 1e6,
              memory_limit / 1e6);
        }
      }
    }

    uint64_t limit_with_pinned = static_cast<uint64_t>(
        memory_limit *
        (1 + getSettings()->pinned_memtables_limit_percent / 100.));
    if (buf_stats.active_memory_usage + buf_stats.memory_being_flushed +
            buf_stats.pinned_buffer_usage >
        limit_with_pinned) {
      new_state = WriteThrottleState::REJECT_WRITE;
    }
  } else {
    // Flushes and most of throttling are managed by rocksdb, but we still need
    // to stall low-pri writes separately and more aggressively.
    // Let's stall all low-pri writes during any flushes. This way we don't need
    // to make any assumptions about rocksdb's flush policy.
    new_state = buf_stats.memory_being_flushed > 0
        ? WriteThrottleState::STALL_LOW_PRI_WRITE
        : WriteThrottleState::NONE;
  }

  new_state = std::max(new_state, subclassSuggestedThrottleState());

  auto now = SteadyTimestamp::now();
  WriteThrottleState prev_state;
  std::chrono::steady_clock::duration prev_state_duration{0};
  std::chrono::steady_clock::duration time_since_last_update{0};

  {
    std::unique_lock<std::mutex> lock(throttle_state_mutex_);
    prev_state = write_throttle_state_.exchange(new_state);
    if (last_throttle_update_time_ != SteadyTimestamp::min()) {
      time_since_last_update = now - last_throttle_update_time_;
    }
    last_throttle_update_time_ = now;
    if (new_state != prev_state) {
      if (write_throttle_state_since_ != SteadyTimestamp::min()) {
        prev_state_duration = now - write_throttle_state_since_;
      }
      write_throttle_state_since_ = now;
    }
  }

  if (prev_state == WriteThrottleState::REJECT_WRITE) {
    PER_SHARD_STAT_ADD(stats_,
                       reject_writes_microsec,
                       shard_idx_,
                       to_usec(time_since_last_update).count());
  } else if (prev_state == WriteThrottleState::STALL_LOW_PRI_WRITE) {
    PER_SHARD_STAT_ADD(stats_,
                       low_pri_write_stall_microsec,
                       shard_idx_,
                       to_usec(time_since_last_update).count());
  }

  if (prev_state != new_state) {
    std::string s = folly::sformat(
        "Shard {}: throttling transitioned from {} to {} after {:.3f}s. "
        "Memtables active: {:.3f} MB, flushing: {:.3f} MB, pinned: {:.3f} MB.",
        getShardIdx(),
        state_to_str(prev_state),
        state_to_str(new_state),
        to_sec_double(prev_state_duration),
        buf_stats.active_memory_usage / 1e6,
        buf_stats.memory_being_flushed / 1e6,
        buf_stats.pinned_buffer_usage / 1e6);
    if (new_state == WriteThrottleState::REJECT_WRITE ||
        prev_state == WriteThrottleState::REJECT_WRITE ||
        getSettings()->print_details) {
      ld_info("%s", s.c_str());
    } else {
      RATELIMIT_INFO(std::chrono::seconds(1), 1, "%s", s.c_str());
    }

    throttle_state_cv_.notify_all();
  }
}

void RocksDBLogStoreBase::disableWriteStalling() {
  {
    std::lock_guard<std::mutex> lock(throttle_state_mutex_);
    disable_stalling_ = true;
  }
  throttle_state_cv_.notify_all();
}

void RocksDBLogStoreBase::stallLowPriWrite() {
  WriteThrottleState throttle = write_throttle_state_.load();

  if (throttle == WriteThrottleState::NONE) {
    return;
  }

  std::unique_lock<std::mutex> lock(throttle_state_mutex_);
  throttle_state_cv_.wait(lock, [&] {
    return disable_stalling_ ||
        write_throttle_state_.load() == WriteThrottleState::NONE;
  });
}

int RocksDBLogStoreBase::readAllLogSnapshotBlobsImpl(
    LogSnapshotBlobType snapshots_type,
    LogSnapshotBlobCallback callback,
    rocksdb::ColumnFamilyHandle* snapshots_cf) {
  ld_check(snapshots_cf);

  auto it = newIterator(getDefaultReadOptions(), snapshots_cf);
  LogSnapshotBlobKey seek_target(snapshots_type, LOGID_INVALID);
  it.Seek(rocksdb::Slice(
      reinterpret_cast<const char*>(&seek_target), sizeof(seek_target)));
  for (; it.status().ok() && it.Valid(); it.Next()) {
    auto key_raw = it.key();
    if (!LogSnapshotBlobKey::valid(
            snapshots_type, key_raw.data(), key_raw.size())) {
      break;
    }

    auto logid = LogSnapshotBlobKey::getLogID(key_raw.data());
    Slice blob = Slice(it.value().data(), it.value().size());
    int rv = callback(logid, blob);
    if (rv != 0) {
      return -1;
    }
  }

  return it.status().ok() ? 0 : -1;
}

rocksdb::ReadOptions RocksDBLogStoreBase::translateReadOptions(
    const LocalLogStore::ReadOptions& opts,
    bool single_log,
    rocksdb::Slice* upper_bound) {
  rocksdb::ReadOptions rocks_options = single_log
      ? RocksDBLogStoreBase::getReadOptionsSinglePrefix()
      : RocksDBLogStoreBase::getDefaultReadOptions();

  rocks_options.fill_cache = opts.fill_cache;
  rocks_options.read_tier =
      opts.allow_blocking_io ? rocksdb::kReadAllTier : rocksdb::kBlockCacheTier;

  // Tailing iterator isn't tied to a snapshot of the database, so using it
  // allows us to cache and reuse the iterator.
  rocks_options.tailing = opts.tailing;

  if (upper_bound != nullptr && !upper_bound->empty()) {
    // Since this iterator is only used to read data for a given log, setting
    // iterate_upper_bound allows RocksDB to release some resources when child
    // iterators move past all the records for this log.
    rocks_options.iterate_upper_bound = upper_bound;
  }

  return rocks_options;
}

int RocksDBLogStoreBase::syncWAL() {
  rocksdb::Status status = writer_->syncWAL();
  if (!status.ok()) {
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }
  return 0;
}

FlushToken RocksDBLogStoreBase::maxWALSyncToken() const {
  return writer_->maxWALSyncToken();
}

FlushToken RocksDBLogStoreBase::walSyncedUpThrough() const {
  return writer_->walSyncedUpThrough();
}

int RocksDBLogStoreBase::readLogMetadata(logid_t log_id,
                                         LogMetadata* metadata) {
  return writer_->readLogMetadata(log_id, metadata, getMetadataCFHandle());
}
int RocksDBLogStoreBase::readStoreMetadata(StoreMetadata* metadata) {
  return writer_->readStoreMetadata(metadata, getMetadataCFHandle());
}
int RocksDBLogStoreBase::readPerEpochLogMetadata(logid_t log_id,
                                                 epoch_t epoch,
                                                 PerEpochLogMetadata* metadata,
                                                 bool find_last_available,
                                                 bool allow_blocking_io) const {
  return writer_->readPerEpochLogMetadata(log_id,
                                          epoch,
                                          metadata,
                                          getMetadataCFHandle(),
                                          find_last_available,
                                          allow_blocking_io);
}

int RocksDBLogStoreBase::writeLogMetadata(logid_t log_id,
                                          const LogMetadata& metadata,
                                          const WriteOptions& write_options) {
  return writer_->writeLogMetadata(
      log_id, metadata, write_options, getMetadataCFHandle());
}
int RocksDBLogStoreBase::writeStoreMetadata(const StoreMetadata& metadata,
                                            const WriteOptions& write_options) {
  return writer_->writeStoreMetadata(
      metadata, write_options, getMetadataCFHandle());
}

int RocksDBLogStoreBase::updateLogMetadata(logid_t log_id,
                                           ComparableLogMetadata& metadata,
                                           const WriteOptions& write_options) {
  return writer_->updateLogMetadata(
      log_id, metadata, write_options, getMetadataCFHandle());
}
int RocksDBLogStoreBase::updatePerEpochLogMetadata(
    logid_t log_id,
    epoch_t epoch,
    PerEpochLogMetadata& metadata,
    LocalLogStore::SealPreemption seal_preempt,
    const WriteOptions& write_options) {
  return writer_->updatePerEpochLogMetadata(log_id,
                                            epoch,
                                            metadata,
                                            seal_preempt,
                                            write_options,
                                            getMetadataCFHandle());
}

int RocksDBLogStoreBase::getRebuildingRanges(RebuildingRangesMetadata& rrm,
                                             RebuildingRangesVersion& version) {
  rrm = RebuildingRangesMetadata();
  version = RebuildingRangesVersion(0, 0);
  return 0;
}

int RocksDBLogStoreBase::writeRebuildingRanges(
    RebuildingRangesMetadata&,
    RebuildingRangesVersion /*base_version*/,
    RebuildingRangesVersion /*new_version*/) {
  err = E::NOTSUPPORTED;
  return -1;
}

int RocksDBLogStoreBase::deleteStoreMetadata(
    const StoreMetadataType type,
    const WriteOptions& write_options) {
  return writer_->deleteStoreMetadata(
      type, write_options, getMetadataCFHandle());
}
int RocksDBLogStoreBase::deleteLogMetadata(logid_t first_log_id,
                                           logid_t last_log_id,
                                           const LogMetadataType type,
                                           const WriteOptions& write_options) {
  return writer_->deleteLogMetadata(
      first_log_id, last_log_id, type, write_options, getMetadataCFHandle());
}
int RocksDBLogStoreBase::deletePerEpochLogMetadata(
    logid_t log_id,
    epoch_t epoch,
    const PerEpochLogMetadataType type,
    const WriteOptions& write_options) {
  return writer_->deletePerEpochLogMetadata(
      log_id, epoch, type, write_options, getMetadataCFHandle());
}

RocksDBCFPtr
RocksDBLogStoreBase::getColumnFamilyPtr(uint32_t column_family_id) {
  RocksDBCFPtr cf_ptr;
  cf_accessor_.withRLock([&](auto& locked_accessor) {
    const auto& iter = locked_accessor.find(column_family_id);
    if (iter != locked_accessor.end()) {
      cf_ptr = iter->second;
    }
  });

  return cf_ptr;
}

rocksdb::Status
RocksDBLogStoreBase::writeBatch(const rocksdb::WriteOptions& options,
                                rocksdb::WriteBatch* batch) {
  if (getSettings()->read_only) {
    ld_check(false);
    err = E::LOCAL_LOG_STORE_WRITE;
    return rocksdb::Status::IOError(
        "assertion failure: trying to write to read-only store");
  }
  using IOType = IOFaultInjection::IOType;
  using FaultType = IOFaultInjection::FaultType;

  auto* perf_context = rocksdb::get_perf_context();
  auto* iostats_context = rocksdb::get_iostats_context();
  uint64_t wal_start = perf_context->write_wal_time;
  uint64_t mem_start = perf_context->write_memtable_time;
  uint64_t delay_start = perf_context->write_delay_time;
  uint64_t scheduling_start =
      ROCKSDB_PERF_COUNTER_write_scheduling_flushes_compactions_time(
          perf_context);
  uint64_t pre_and_post_start = perf_context->write_pre_and_post_process_time;

  uint64_t wait_start =
      ROCKSDB_PERF_COUNTER_write_thread_wait_nanos(perf_context);
  uint64_t mutex_start = perf_context->db_mutex_lock_nanos;
  uint64_t cv_start = perf_context->db_condition_wait_nanos;
  uint64_t open_start = iostats_context->open_nanos;
  uint64_t allocate_start = iostats_context->allocate_nanos;
  uint64_t write_start = iostats_context->write_nanos;
  uint64_t range_sync_start = iostats_context->range_sync_nanos;
  uint64_t logger_start = iostats_context->logger_nanos;

  auto time_start = std::chrono::steady_clock::now();

  rocksdb::Status status;
  shard_index_t shard_idx = getShardIdx();
  auto& io_fault_injection = IOFaultInjection::instance();
  auto fault = io_fault_injection.getInjectedFault(
      shard_idx, IOType::WRITE, FaultType::CORRUPTION | FaultType::IO_ERROR);
  if (fault != FaultType::NONE) {
    status = RocksDBLogStoreBase::FaultTypeToStatus(fault);
    ld_check(!status.ok());
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Returning injected error %s for shard %d.",
                    status.ToString().c_str(),
                    getShardIdx());
    // Don't bump error stats for injected errors.
    enterFailSafeMode("Write()", "injected error");
  } else {
    status = getDB().Write(options, batch);
    enterFailSafeIfFailed(status, "Write()");
  }

  if (shard_idx != -1 && status.ok()) {
    // RocksDB keeps track of time spent in nanoseconds
    uint64_t wal_nanos = perf_context->write_wal_time - wal_start;
    uint64_t mem_nanos = perf_context->write_memtable_time - mem_start;
    uint64_t delay_nanos = perf_context->write_delay_time - delay_start;
    uint64_t scheduling_nanos =
        ROCKSDB_PERF_COUNTER_write_scheduling_flushes_compactions_time(
            perf_context) -
        scheduling_start;
    uint64_t pre_and_post_nanos =
        perf_context->write_pre_and_post_process_time - pre_and_post_start;

    PER_SHARD_HISTOGRAM_ADD(
        getStatsHolder(), rocks_wal, shard_idx, wal_nanos / 1000);
    PER_SHARD_HISTOGRAM_ADD(
        getStatsHolder(), rocks_memtable, shard_idx, mem_nanos / 1000);
    PER_SHARD_HISTOGRAM_ADD(
        getStatsHolder(), rocks_delay, shard_idx, delay_nanos / 1000);
    PER_SHARD_HISTOGRAM_ADD(
        getStatsHolder(), rocks_scheduling, shard_idx, scheduling_nanos / 1000);
    PER_SHARD_HISTOGRAM_ADD(getStatsHolder(),
                            rocks_pre_and_post,
                            shard_idx,
                            pre_and_post_nanos / 1000);

    auto time_end = std::chrono::steady_clock::now();

    uint64_t wait_nanos =
        ROCKSDB_PERF_COUNTER_write_thread_wait_nanos(perf_context) - wait_start;
    uint64_t mutex_nanos = perf_context->db_mutex_lock_nanos - mutex_start;
    uint64_t cv_nanos = perf_context->db_condition_wait_nanos - cv_start;
    uint64_t open_nanos = iostats_context->open_nanos - open_start;
    uint64_t allocate_nanos = iostats_context->allocate_nanos - allocate_start;
    uint64_t write_nanos = iostats_context->write_nanos - write_start;
    uint64_t range_sync_nanos =
        iostats_context->range_sync_nanos - range_sync_start;
    uint64_t logger_nanos = iostats_context->logger_nanos - logger_start;

    std::chrono::nanoseconds total_time = time_end - time_start;

    if (total_time > std::chrono::milliseconds(500)) {
      uint64_t total_nanos = total_time.count();
      uint64_t explained_nanos = wait_nanos + mutex_nanos + cv_nanos +
          open_nanos + allocate_nanos + write_nanos + range_sync_nanos +
          logger_nanos;
      int64_t unexplained_nanos =
          (int64_t)total_nanos - (int64_t)explained_nanos;
      ld_info("slow rocksdb::DB::Write() for shard %d; %d ops, %lu bytes; "
              "total: %.6fs; WAL: %.6fs, Memtable: %.6fs, Delay: %.6fs, "
              "Scheduling flushes/compactions: %.6fs, Pre-and-post: %.6fs; "
              "lowlevel: wait for batch: %.6fs, mutex: %.6fs, cv: %.6fs, "
              "open(): %.6fs, fallocate(): %.6fs, write(): %.6fs, "
              "sync_file_range(): %.6fs, logger: %.6fs, other: %.6fs",
              shard_idx,
              batch->Count(),
              batch->GetDataSize(),
              total_nanos / 1e9,
              wal_nanos / 1e9,
              mem_nanos / 1e9,
              delay_nanos / 1e9,
              scheduling_nanos / 1e9,
              pre_and_post_nanos / 1e9,
              wait_nanos / 1e9,
              mutex_nanos / 1e9,
              cv_nanos / 1e9,
              open_nanos / 1e9,
              allocate_nanos / 1e9,
              write_nanos / 1e9,
              range_sync_nanos / 1e9,
              logger_nanos / 1e9,
              unexplained_nanos / 1e9);
    }
  }

  if (!status.ok()) {
    ld_debug("In failsafemode for shard_idx:%d, status=%s",
             shard_idx,
             status.ToString().c_str());
    PER_SHARD_STAT_INCR(
        getStatsHolder(), local_logstore_failed_writes, getShardIdx());
  }

  return status;
}

// Instrumentation wrapper for iterator operations.
// Checks some thread-local counters and prints a warning if the operation was
// way too expensive.
//
// Create just before the operation, destroy right after.
//
// RocksDB iterators sometimes need to do linear search to skip over
// deleted/merged/hidden keys. If we're not careful about how we write to
// rocksdb, iterators may end up doing lots of such linear search.
// E.g. if you write lots of keys to memtable, then delete all of them, the
// memtable will still contain all the keys and their deletion markers. If you
// then seek an iterator to the first of these keys, the iterator will iterate
// over all these keys and deletion markers before it reaches a non-deleted key
// or the end. Similarly, if you write, then flush memtable, then delete, then
// flush again - the writes and deletes will be in different sst files, so
// iterator will have no way to skip the deleted range without stepping through
// all the keys; compaction cleans it up.
//
// Counters we look at:
//  - internal_merge_count - how many merge operands were processed.
//    Can be big if some key either has lots of merges in memtable
//    (i.e. Merge() was called lots of times for same key since last flush), or
//    has lots of sst files each having one merge for this key (merges are
//    collapsed when writing sst file, so each file has at most one merge
//    operand per key).
//  - internal_delete_skipped_count - how many deletion tombstones were
//    processed. Can be big if lots of consecutive keys have been deleted (one
//    by one, using Delete()), and these deletes are either in memtable or in a
//    different sst file than the corresponding puts.
//  - internal_key_skipped_count - how many keys were skipped for other reasons:
//    (a) key-values that have deletion tombstones (the tombstones are counted
//    by the other counter, the values deleted by those tombstones are counted
//    here), (b) overwritten values in memtable (if you do many Put()s for the
//    same key, memtable will keep all versions, and iterator needs to skip them
//    to go to the next key; after 8 steps it gives up and does a seek to the
//    next key), (c) maybe other, smaller, cases.
//  - recent_skipped_before - how many key-values in memtable were skipped
//    because they were written after the snapshot iterator was created.
namespace {
struct WarnIfTooManySkips {
  const char* op_name;
  rocksdb::PerfContext* perf_context;
  uint64_t merge_before;
  uint64_t delete_skipped_before;
  uint64_t key_skipped_before;
  uint64_t recent_skipped_before;

  explicit WarnIfTooManySkips(const char* op) {
    op_name = op;
    perf_context = rocksdb::get_perf_context();
    merge_before = perf_context->internal_merge_count;
    delete_skipped_before = perf_context->internal_delete_skipped_count;
    key_skipped_before = perf_context->internal_key_skipped_count;
    recent_skipped_before = perf_context->internal_recent_skipped_count;
  }

  ~WarnIfTooManySkips() {
    uint64_t merge = perf_context->internal_merge_count - merge_before;
    uint64_t delete_skipped =
        perf_context->internal_delete_skipped_count - delete_skipped_before;
    uint64_t key_skipped =
        perf_context->internal_key_skipped_count - key_skipped_before;
    uint64_t recent_skipped =
        perf_context->internal_recent_skipped_count - recent_skipped_before;
    if (merge + delete_skipped + key_skipped + recent_skipped > 1000) {
      RATELIMIT_WARNING(
          std::chrono::seconds(2),
          1,
          "Iterator %s did way too many steps: %lu merge operands, %lu delete "
          "markers, %lu deleted values, %lu post-snapshot values",
          op_name,
          merge,
          delete_skipped,
          key_skipped,
          recent_skipped);
    }
  }
};
} // namespace

void RocksDBIterator::SeekToFirst() {
  ld_check(iterator_ != nullptr);
  valid_checked_ = false;
  status_checked_ = false;

  {
    WarnIfTooManySkips("SeekToFirst");
    iterator_->SeekToFirst();
  }

  status_ = getRocksdbStatus();
  store_->enterFailSafeIfFailed(status_.value(), "SeekToFirst()");
}

void RocksDBIterator::SeekToLast() {
  ld_check(iterator_ != nullptr);
  valid_checked_ = false;
  status_checked_ = false;

  {
    WarnIfTooManySkips("SeekToLast");
    iterator_->SeekToLast();
  }

  status_ = getRocksdbStatus();
  store_->enterFailSafeIfFailed(status_.value(), "SeekToLast()");
}

void RocksDBIterator::Seek(const rocksdb::Slice& target) {
  ld_check(iterator_ != nullptr);
  valid_checked_ = false;
  status_checked_ = false;

  {
    WarnIfTooManySkips("Seek");
    iterator_->Seek(target);
  }

  status_ = getRocksdbStatus();
  store_->enterFailSafeIfFailed(status_.value(), "Seek()");
}

void RocksDBIterator::SeekForPrev(const rocksdb::Slice& target) {
  ld_check(iterator_ != nullptr);
  valid_checked_ = false;
  status_checked_ = false;

  {
    WarnIfTooManySkips("SeekForPrev");
    iterator_->SeekForPrev(target);
  }

  status_ = getRocksdbStatus();
  store_->enterFailSafeIfFailed(status_.value(), "SeekForPrev()");
}

void RocksDBIterator::Next() {
  ld_check(iterator_ != nullptr);
  ld_check(valid_checked_);
  ld_check(status_checked_);
  valid_checked_ = false;
  status_checked_ = false;

  {
    WarnIfTooManySkips("Next");
    iterator_->Next();
  }

  status_ = getRocksdbStatus();
  store_->enterFailSafeIfFailed(status_.value(), "Next()");
}

void RocksDBIterator::Prev() {
  ld_check(iterator_ != nullptr);
  ld_check(valid_checked_);
  ld_check(status_checked_);
  valid_checked_ = false;
  status_checked_ = false;

  {
    WarnIfTooManySkips("Prev");
    iterator_->Prev();
  }

  status_ = getRocksdbStatus();
  store_->enterFailSafeIfFailed(status_.value(), "Prev()");
}

void RocksDBIterator::Refresh() {
  ld_check(iterator_ != nullptr);
  valid_checked_ = false;
  status_checked_ = false;

  iterator_->Refresh();

  status_ = getRocksdbStatus();
  store_->enterFailSafeIfFailed(status_.value(), "Refresh()");
}

rocksdb::Status RocksDBIterator::getRocksdbStatus() {
  using IOType = IOFaultInjection::IOType;
  using DataType = IOFaultInjection::DataType;
  using FaultType = IOFaultInjection::FaultType;

  rocksdb::Status status;
  auto* rb_store = static_cast<const RocksDBLogStoreBase*>(store_);
  auto& io_fault_injection = IOFaultInjection::instance();
  auto sim_error = io_fault_injection.getInjectedFault(
      store_->getShardIdx(),
      IOType::READ,
      FaultType::IO_ERROR | FaultType::CORRUPTION,
      DataType::DATA);
  if (sim_error != FaultType::NONE) {
    status = RocksDBLogStoreBase::FaultTypeToStatus(sim_error);
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    2,
                    "Returning injected error '%s' for shard %d.",
                    status.ToString().c_str(),
                    rb_store->getShardIdx());
  } else {
    status = iterator_->status();
  }
  return status;
}

int RocksDBLogStoreBase::traverseLogsMetadata(
    LogMetadataType type,
    LocalLogStore::TraverseLogsMetadataCallback cb) {
  using RocksDBKeyFormat::LogMetaKey;

  LogMetaKey first_key(type, logid_t(0));
  rocksdb::ReadOptions read_options;
  RocksDBIterator it = newIterator(read_options, getMetadataCFHandle());

  it.Seek(rocksdb::Slice(
      reinterpret_cast<const char*>(&first_key), sizeof(first_key)));

  for (; it.status().ok() && it.Valid(); it.Next()) {
    Status status = E::OK;
    auto key = it.key();

    if (!(key.size() > 0 && key[0] == LogMetaKey::getHeader(type))) {
      break;
    }
    // Special handling is needed for LogMetadataType::SEAL. See T39174994.
    if (type == LogMetadataType::SEAL &&
        it.key().compare(
            rocksdb::Slice(RocksDBLogStoreBase::OLD_SCHEMA_VERSION_KEY)) == 0) {
      continue;
    }
    auto value = it.value();
    if (!LogMetaKey::valid(type, key.data(), key.size())) {
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "Malformed metadata key. Key: %s, Value: %s",
                         hexdump_buf(key.data(), key.size()).c_str(),
                         hexdump_buf(value.data(), value.size()).c_str());

      // If the key is malformed, we won't know the `log_id` for
      // invoking the callback function. Hence, abandon this shard.
      err = E::LOCAL_LOG_STORE_READ;
      return -1;
    }

    logid_t log_id = LogMetaKey::getLogID(key.data());
    auto meta = LogMetadataFactory::create(type);
    if (meta->deserialize(Slice(value.data(), value.size())) != 0) {
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "Malformed metadata value: Key: %s, Value: %s",
                         hexdump_buf(key.data(), key.size()).c_str(),
                         hexdump_buf(value.data(), value.size()).c_str());
      // If the value is malformed, we at least know the `log_id`. Let
      // caller know about it.
      status = E::MALFORMED_RECORD;
    }
    cb(log_id, status == E::OK ? std::move(meta) : nullptr, status);
  }

  if (!it.status().ok()) {
    err = E::LOCAL_LOG_STORE_READ;
    return -1;
  }
  return 0;
}

}} // namespace facebook::logdevice
