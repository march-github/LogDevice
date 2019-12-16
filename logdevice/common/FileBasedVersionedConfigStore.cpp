/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/FileBasedVersionedConfigStore.h"

#include <shared_mutex>

#include <boost/filesystem.hpp>
#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/Singleton.h>
#include <folly/synchronization/Baton.h>

#include "logdevice/common/ThreadID.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

FileBasedVersionedConfigStore::FileBasedVersionedConfigStore(
    std::string root_path,
    extract_version_fn f)
    : VersionedConfigStore(std::move(f)),
      root_path_(std::move(root_path)),
      task_queue_(QUEUE_SIZE) {
  // start task threads
  for (auto i = 0; i < NUM_THREADS; ++i) {
    task_threads_.emplace_back([this] { threadMain(); });
  }
  ld_info("FileBasedVersionedConfigStore threads started.");
}

void FileBasedVersionedConfigStore::stopAndJoin() {
  // enqueue the termination task for each worker
  for (auto i = 0; i < NUM_THREADS; ++i) {
    task_queue_.blockingWrite(folly::Function<void()>());
  }
  for (auto& t : task_threads_) {
    t.join();
  }
  ld_info("FileBasedVersionedConfigStore threads stopped.");
}

void FileBasedVersionedConfigStore::shutdown() {
  // Assumptions:
  // 1) shutdown should only be called on one thread;
  // 2) when shutdown is called, there should be no thread context still
  //    calling FileBasedVersionedConfigStore public functions
  bool has_shutdown = shutdown_signaled_.exchange(true);
  if (!has_shutdown) {
    stopAndJoin();
  }
}

FileBasedVersionedConfigStore::~FileBasedVersionedConfigStore() {
  shutdown();
}

void FileBasedVersionedConfigStore::threadMain() {
  ThreadID::set(ThreadID::Type::UTILITY, "ld:file-ncs");
  while (true) {
    folly::Function<void()> task;
    task_queue_.blockingRead(task);
    if (!task) {
      // store is shutting down
      ld_check(shutdown_signaled_.load());
      return;
    }
    task();
  }
}

void FileBasedVersionedConfigStore::getConfigImpl(
    std::string key,
    value_callback_t::SharedProxy cb,
    folly::Optional<version_t> base_version) const {
  if (shutdown_signaled_.load()) {
    cb(E::SHUTDOWN, "");
    return;
  }

  folly::File lock_file;
  try {
    auto lock_path = getLockFilePath(key);
    createDirectoriesOfFile(lock_path);
    lock_file = folly::File(lock_path, O_RDWR | O_CREAT | O_CLOEXEC, 0700);
  } catch (const std::exception& ex) {
    ld_error("Failed to create lockfile %s: %s",
             getLockFilePath(key).c_str(),
             folly::exceptionStr(ex).toStdString().c_str());
    cb(E::ACCESS, "");
    return;
  }

  std::shared_lock<folly::File> shared_flock(lock_file);
  folly::File data_file;
  try {
    data_file = folly::File(getDataFilePath(key), O_RDONLY, 0700);
  } catch (const std::exception& ex) {
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      5,
                      "Failed to open data file %s: %s",
                      getDataFilePath(key).c_str(),
                      folly::exceptionStr(ex).c_str());
    cb(E::NOTFOUND, "");
    return;
  }

  std::string value;
  if (!folly::readFile(data_file.fd(), value, MAX_VALUE_SIZE_IN_BYTES)) {
    ld_error("Failed to read data file %s: %s",
             getDataFilePath(key).c_str(),
             strerror(errno));
    cb(E::ACCESS, "");
    return;
  }

  if (base_version.hasValue()) {
    auto current_version_opt = (extract_fn_)(value);
    if (!current_version_opt) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        5,
                        "Failed to extract version from value read from "
                        "FileBasedVersionedConfigurationStore. key: \"%s\"",
                        key.c_str());
      cb(E::BADMSG, "");
      return;
    }
    if (current_version_opt.value() <= base_version.value()) {
      // file's config version is not larger than the base version
      cb(E::UPTODATE, "");
      return;
    }
  }

  cb(E::OK, std::move(value));
}

void FileBasedVersionedConfigStore::updateConfigImpl(
    std::string key,
    std::string value,
    version_t new_version,
    Condition base_version,
    write_callback_t::SharedProxy cb) {
  if (shutdown_signaled_.load()) {
    cb(E::SHUTDOWN, {}, "");
    return;
  }

  if (value.size() > MAX_VALUE_SIZE_IN_BYTES) {
    cb(E::INVALID_PARAM, {}, "");
    return;
  }

  folly::File lock_file;
  try {
    auto lock_path = getLockFilePath(key);
    createDirectoriesOfFile(lock_path);
    lock_file = folly::File(lock_path, O_RDWR | O_CREAT);
  } catch (const std::exception& ex) {
    ld_error("Failed to create lockfile %s: %s",
             getLockFilePath(key).c_str(),
             folly::exceptionStr(ex).toStdString().c_str());
    cb(E::ACCESS, {}, "");
    return;
  }

  // exlusive lock on write path
  std::lock_guard<folly::File> exclusive_flock(lock_file);
  folly::File data_file;
  const std::string data_file_path = getDataFilePath(key);
  try {
    // open file in read-only but with on-demand creation, for writes, we will
    // open the file again if needed
    createDirectoriesOfFile(data_file_path);
    data_file = folly::File(data_file_path, O_RDONLY | O_CREAT, 0600);
  } catch (const std::exception& ex) {
    ld_error("Failed to open or create data file %s: %s",
             data_file_path.c_str(),
             folly::exceptionStr(ex).toStdString().c_str());
    cb(E::ACCESS, {}, "");
    return;
  }

  std::string current_value;
  if (!folly::readFile(
          data_file.fd(), current_value, MAX_VALUE_SIZE_IN_BYTES)) {
    ld_error("Failed to read data file %s: %s",
             data_file_path.c_str(),
             strerror(errno));
    cb(E::ACCESS, {}, "");
    return;
  }

  folly::Optional<version_t> current_version_opt;
  if (!current_value.empty()) {
    current_version_opt = (extract_fn_)(current_value);
    if (!current_version_opt) {
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          5,
          "Failed to extract version from value read. key: \"%s\"",
          key.c_str());
      cb(E::BADMSG, {}, "");
      return;
    }
  }
  if (auto status = isAllowedUpdate(base_version, current_version_opt);
      status != E::OK) {
    // version conditional update failed, invoke the callback with the
    // version and value that are more recent
    cb(std::move(status),
       current_version_opt.value_or(version_t(0)),
       std::move(current_value));
    return;
  }

  // close the read-only fd so that we could write to it
  data_file.closeNoThrow();

  try {
    // write the new value atomically, internally it writes to a tmp file and
    // then rename
    folly::writeFileAtomic(data_file_path, value);
  } catch (const std::exception& ex) {
    ld_error("Failed to atomically write data file %s: %s",
             data_file_path.c_str(),
             folly::exceptionStr(ex).toStdString().c_str());
    cb(E::ACCESS, {}, "");
    return;
  }

  cb(E::OK, std::move(new_version), "");
}

void FileBasedVersionedConfigStore::getConfig(
    std::string key,
    value_callback_t cb,
    folly::Optional<version_t> base_version) const {
  if (shutdown_signaled_.load()) {
    cb(E::SHUTDOWN, {});
    return;
  }

  auto cb_shared = std::move(cb).asSharedProxy();
  bool success = task_queue_.writeIfNotFull([this,
                                             key = std::move(key),
                                             cb_shared = cb_shared,
                                             base_version]() mutable {
    getConfigImpl(std::move(key), cb_shared, base_version);
  });
  if (!success) {
    // Queue full, report transient error.
    // `func` was not moved out of, so it wasn't destroyed and wasn't called,
    // so cb_plain is still alive.
    cb_shared(E::AGAIN, {});
    return;
  }
}

void FileBasedVersionedConfigStore::getLatestConfig(std::string key,
                                                    value_callback_t cb) const {
  static_assert(
      NUM_THREADS == 1,
      "getLatestConfig depends on the FileBasedVersionedConfigStore to be "
      "single threaded to provide the linearizability. If this ever going to "
      "change, please make sure that the linearizability gurantees are still "
      "respected in the new model.");
  getConfig(std::move(key), std::move(cb));
}

void FileBasedVersionedConfigStore::readModifyWriteConfig(
    std::string key,
    mutation_callback_t mcb,
    write_callback_t cb) {
  if (shutdown_signaled_.load()) {
    cb(E::SHUTDOWN, {}, "");
    return;
  }

  std::string current_value;
  auto status = getConfigSync(key, &current_value);
  if (status != E::OK && status != E::NOTFOUND) {
    cb(status, version_t{}, "");
    return;
  }

  folly::Optional<version_t> cur_ver = folly::none;
  if (status == E::OK) {
    auto curr_version_opt = extract_fn_(current_value);
    if (!curr_version_opt) {
      cb(E::BADMSG, version_t{}, "");
      return;
    }
    cur_ver = curr_version_opt.value();
  }

  auto status_value =
      (mcb)((status == E::NOTFOUND)
                ? folly::none
                : folly::Optional<std::string>(std::move(current_value)));
  auto& write_value = status_value.second;
  if (status_value.first != E::OK) {
    cb(status_value.first, version_t{}, std::move(write_value));
    return;
  }

  auto opt = (extract_fn_)(write_value);
  if (!opt) {
    cb(E::INVALID_PARAM, {}, "");
    return;
  }
  version_t new_version = opt.value();

  // TODO: Add stricter enforcement of monotonic increment of version.
  if (cur_ver.hasValue() && new_version.val() <= cur_ver.value().val()) {
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      5,
                      "Config value's version is not monitonically increasing"
                      "key: \"%s\". prev version: \"%lu\". version: \"%lu\"",
                      key.c_str(),
                      cur_ver.value().val(),
                      new_version.val());
  }

  auto cb_shared = std::move(cb).asSharedProxy();
  bool success = task_queue_.writeIfNotFull([this,
                                             key = std::move(key),
                                             value = std::move(write_value),
                                             base_version = std::move(cur_ver),
                                             new_version =
                                                 std::move(new_version),
                                             cb_shared = cb_shared]() mutable {
    updateConfigImpl(std::move(key),
                     std::move(value),
                     std::move(new_version),
                     base_version.hasValue()
                         ? base_version.value()
                         : VersionedConfigStore::Condition::createIfNotExists(),
                     cb_shared);
  });
  if (!success) {
    // queue full, report transient error
    cb_shared(E::AGAIN, {}, "");
    return;
  }
}

void FileBasedVersionedConfigStore::createDirectoriesOfFile(
    const std::string& file) {
  boost::filesystem::path p(file);
  boost::filesystem::create_directories(p.parent_path());
}

}} // namespace facebook::logdevice
