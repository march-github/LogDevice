/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <utility>

#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SequencerBatching.h"
#include "logdevice/common/TrafficShaper.h"
#include "logdevice/common/settings/GossipSettings.h"
#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/HealthMonitor.h"
#include "logdevice/server/LocalLogFile.h"
#include "logdevice/server/ServerSettings.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/WatchDogThread.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/sequencer_boycotting/BoycottingStats.h"

/**
 * @file Subclass of Processor containing state specific to servers, also
 * spawning ServerWorker instances instead of plain Worker.
 */

namespace facebook { namespace logdevice {

class ShardedStorageThreadPool;

class ServerProcessor : public Processor {
 public:
  // Factory method just forwards to the private constructor.  We do this to
  // ensure init() gets called subsequently, allowing the base class to call
  // virtual methods to complete initialization.
  template <typename... Args>
  static std::shared_ptr<ServerProcessor> create(Args&&... args) {
    auto p = std::make_shared<ServerProcessor>(std::forward<Args>(args)...);
    p->init();
    p->startRunning();
    return p;
  }

  // Like create(), but you'll have to call startRunning() on the returned
  // Processor to start workers.
  template <typename... Args>
  static std::shared_ptr<ServerProcessor>
  createWithoutStarting(Args&&... args) {
    auto p = std::make_shared<ServerProcessor>(std::forward<Args>(args)...);
    p->init();
    return p;
  }

  ServerWorker* createWorker(WorkContext::KeepAlive executor,
                             worker_id_t i,
                             WorkerType type) override;

  void applyToWorkers(folly::Function<void(ServerWorker&)> func,
                      Order order = Order::FORWARD) {
    Processor::applyToWorkers(
        [&func](Worker& worker) {
          func(checked_downcast<ServerWorker&>(worker));
        },
        order);
  }

  /**
   * Returns the reference to the worker instance with the given index.
   */
  ServerWorker& getWorker(worker_id_t worker_id, WorkerType type) {
    return checked_downcast<ServerWorker&>(
        Processor::getWorker(worker_id, type));
  }

  const std::shared_ptr<LocalLogFile>& getAuditLog() {
    return audit_log_;
  }

  LogStorageStateMap& getLogStorageStateMap() const;

  // Alternative factory for tests that need to construct a half-baked
  // Processor (no workers etc).
  template <typename... Args>
  static std::unique_ptr<ServerProcessor> createNoInit(Args&&... args) {
    return std::unique_ptr<ServerProcessor>(
        new ServerProcessor(std::forward<Args>(args)...));
  }

  void init() override;

  void startRunning() override;

  int getWorkerCount(WorkerType type = WorkerType::GENERAL) const override;

  bool runningOnStorageNode() const override {
    return Processor::runningOnStorageNode() ||
        sharded_storage_thread_pool_ != nullptr;
  }

  UpdateableSettings<ServerSettings> updateableServerSettings() {
    return server_settings_;
  }

  void getClusterDeadNodeStats(size_t* effective_dead_cnt,
                               size_t* effective_cluster_size) override;

  virtual bool isNodeAlive(node_index_t index) const override;

  virtual bool isNodeBoycotted(node_index_t index) const override;

  virtual bool isNodeIsolated() const override;

  virtual bool isFailureDetectorRunning() const override;
  // Pointer to sharded storage thread pool, if this is a storage node
  // (nullptr if not).  Unowned.
  ShardedStorageThreadPool* const sharded_storage_thread_pool_;

  template <typename... Args>
  ServerProcessor(std::shared_ptr<LocalLogFile> audit_log,
                  ShardedStorageThreadPool* const sharded_storage_thread_pool,
                  std::unique_ptr<LogStorageStateMap> log_storage_state_map,
                  UpdateableSettings<ServerSettings> server_settings,
                  UpdateableSettings<GossipSettings> gossip_settings,
                  UpdateableSettings<AdminServerSettings> admin_server_settings,
                  Args&&... args)
      : Processor(std::forward<Args>(args)...),
        sharded_storage_thread_pool_(sharded_storage_thread_pool),
        audit_log_(audit_log),
        server_settings_(std::move(server_settings)),
        gossip_settings_(std::move(gossip_settings)),
        admin_server_settings_(std::move(admin_server_settings)),
        log_storage_state_map_(std::move(log_storage_state_map)),
        boycotting_stats_(
            updateableSettings()
                ->sequencer_boycotting.node_stats_retention_on_nodes) {
    fixupLogStorageStateMap();
  }

  ~ServerProcessor() override;
  std::unique_ptr<FailureDetector> failure_detector_;

  BoycottingStatsHolder* getBoycottingStats() {
    return &boycotting_stats_;
  }

  void shutdown() override;

  HealthMonitor* getHealthMonitor() {
    return health_monitor_.get();
  }

 private:
  void fixupLogStorageStateMap();

  std::shared_ptr<LocalLogFile> audit_log_;
  UpdateableSettings<ServerSettings> server_settings_;
  UpdateableSettings<GossipSettings> gossip_settings_;
  UpdateableSettings<AdminServerSettings> admin_server_settings_;
  std::unique_ptr<LogStorageStateMap> log_storage_state_map_;
  // node stats sent from the clients. Keep it in a map to be able to identify
  // the client who sent it.
  BoycottingStatsHolder boycotting_stats_;
  // A thread running on server side to detect worker stalls
  std::unique_ptr<WatchDogThread> watchdog_thread_;

  // Orchestrates bandwidth policy and bandwidth releases to the Senders
  // in each Worker.
  std::unique_ptr<TrafficShaper> traffic_shaper_;

  // HealthMonitor pointer. Used on server side to keep track of node status.
  std::unique_ptr<HealthMonitor> health_monitor_;
};
}} // namespace facebook::logdevice
