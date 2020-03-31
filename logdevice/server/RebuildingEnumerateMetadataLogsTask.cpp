/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RebuildingEnumerateMetadataLogsTask.h"

#include "logdevice/common/LegacyLogToShard.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/RocksDBLocalLogStore.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

void RebuildingEnumerateMetadataLogsTask::execute() {
  shard_index_t shard_idx = storageThreadPool_->getShardIdx();

  auto it = storageThreadPool_->getLocalLogStore().readAllLogs(
      LocalLogStore::ReadOptions("RebuildingEnumerateMetadataLogsTask", true),
      /* data_logs_filter */
      std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>{});
  logid_t prev = LOGID_INVALID;
  for (it->seek(*it->metadataLogsBegin(), nullptr, nullptr);
       it->state() == IteratorState::AT_RECORD &&
       MetaDataLog::isMetaDataLog(it->getLogID());
       it->next(nullptr, nullptr)) {
    logid_t l = it->getLogID();
    if (l == LOGID_INVALID) {
      // finish with an error
      status_ = E::LOCAL_LOG_STORE_READ;
      return;
    }
    ld_check(l.val() >= prev.val());
    if (l != prev) {
      const shard_index_t expected = getLegacyShardIndexForLog(l, num_shards_);
      if (expected == shard_idx) {
        result_.push_back(l);
      } else {
        // This should create an alarm for an engineer to investigate.
        ld_critical("Found unexpected metadata log id %lu while enumerating "
                    "logs on shard %u. This log should be on shard %u",
                    l.val(),
                    shard_idx,
                    expected);
      }
      prev = l;
    }
  }
  ld_info("Enumerator queued %ld metadata logs for rebuild", result_.size());

  switch (it->state()) {
    case IteratorState::AT_END:
    case IteratorState::AT_RECORD:
      status_ = E::OK;
      return;
    case IteratorState::ERROR:
      status_ = E::LOCAL_LOG_STORE_READ;
      return;
    case IteratorState::WOULDBLOCK:
    case IteratorState::LIMIT_REACHED:
    case IteratorState::MAX:
      ld_check(false);
      status_ = E::FAILED;
  }

  if (status_ == E::LOCAL_LOG_STORE_READ) {
    storageThreadPool_->getLocalLogStore().enterFailSafeMode(
        "RebuildingEnumerateMetadataLogsTask", "iterator error");
  }
}

void RebuildingEnumerateMetadataLogsTask::onDone() {
  if (!ref_) {
    return;
  }
  shard_index_t shard_idx = storageThreadPool_->getShardIdx();
  ref_->onMetaDataLogsStorageTaskDone(status_, shard_idx, std::move(result_));
}

void RebuildingEnumerateMetadataLogsTask::onDropped() {
  ld_check(false);
}

}} // namespace facebook::logdevice
