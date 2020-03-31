/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/test/TestUtil.h"

#include "logdevice/admin/AdminServer.h"
#include "logdevice/admin/maintenance/ClusterMaintenanceStateMachine.h"
#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/settings/GossipSettings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/settings/util.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/Listener.h"
#include "logdevice/server/LogStoreMonitor.h"
#include "logdevice/server/RebuildingCoordinator.h"
#include "logdevice/server/RebuildingSupervisor.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"
#include "logdevice/server/shutdown.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

TestServerProcessorBuilder::TestServerProcessorBuilder(const Settings& settings)
    : settings_{UpdateableSettings<Settings>(settings)} {}

TestServerProcessorBuilder& TestServerProcessorBuilder::setServerSettings(
    const ServerSettings& server_settings) {
  server_settings_ = UpdateableSettings<ServerSettings>(server_settings);
  return *this;
}

TestServerProcessorBuilder& TestServerProcessorBuilder::setGossipSettings(
    const GossipSettings& gossip_settings) {
  gossip_settings_ = UpdateableSettings<GossipSettings>(gossip_settings);
  return *this;
}

TestServerProcessorBuilder& TestServerProcessorBuilder::setAdminServerSettings(
    const AdminServerSettings& admin_settings) {
  admin_settings_ = UpdateableSettings<AdminServerSettings>(admin_settings);
  return *this;
}

TestServerProcessorBuilder& TestServerProcessorBuilder::setUpdateableConfig(
    std::shared_ptr<UpdateableConfig> config) {
  config_ = config;
  return *this;
}

TestServerProcessorBuilder&
TestServerProcessorBuilder::setShardedStorageThreadPool(
    ShardedStorageThreadPool* sharded_storage_thread_pool) {
  sharded_storage_thread_pool_ = sharded_storage_thread_pool;
  return *this;
}

TestServerProcessorBuilder&
TestServerProcessorBuilder::setStatsHolder(StatsHolder* stats) {
  stats_ = stats;
  return *this;
}

TestServerProcessorBuilder&
TestServerProcessorBuilder::setMyNodeID(NodeID my_node_id) {
  my_node_id_ = my_node_id;
  return *this;
}

TestServerProcessorBuilder& TestServerProcessorBuilder::setDeferStart() {
  defer_start_ = true;
  return *this;
}

std::shared_ptr<ServerProcessor> TestServerProcessorBuilder::build() && {
  if (!config_) {
    setUpdateableConfig(UpdateableConfig::createEmpty());
  }
  if (!server_settings_.has_value()) {
    setServerSettings(create_default_settings<ServerSettings>());
  }
  if (!admin_settings_.has_value()) {
    setAdminServerSettings(create_default_settings<AdminServerSettings>());
  }
  if (!gossip_settings_.has_value()) {
    GossipSettings gossip_settings(create_default_settings<GossipSettings>());
    gossip_settings.enabled = false;
    setGossipSettings(std::move(gossip_settings));
  }
  auto p = ServerProcessor::createWithoutStarting(
      /* audit log */ nullptr,
      sharded_storage_thread_pool_,
      /* log storage state map */ nullptr,
      std::move(server_settings_).value(),
      std::move(gossip_settings_).value(),
      std::move(admin_settings_).value(),
      config_,
      std::make_shared<NoopTraceLogger>(config_),
      std::move(settings_),
      stats_,
      make_test_plugin_registry(),
      "",
      "",
      "logdevice",
      std::move(my_node_id_));

  if (!defer_start_) {
    p->startRunning();
  }

  return p;
}

void shutdown_test_server(std::shared_ptr<ServerProcessor>& processor) {
  std::unique_ptr<AdminServer> admin_handle;
  std::unique_ptr<Listener> connection_listener;
  std::unique_ptr<Listener> gossip_listener;
  std::unique_ptr<Listener> ssl_connection_listener;
  std::unique_ptr<Listener> server_to_server_listener;
  std::unique_ptr<folly::EventBaseThread> connection_listener_loop;
  std::unique_ptr<folly::EventBaseThread> gossip_listener_loop;
  std::unique_ptr<folly::EventBaseThread> ssl_connection_listener_loop;
  std::unique_ptr<folly::EventBaseThread> server_to_server_listener_loop;
  std::unique_ptr<LogStoreMonitor> logstore_monitor;
  std::unique_ptr<ShardedStorageThreadPool> storage_thread_pool;
  std::unique_ptr<ShardedRocksDBLocalLogStore> sharded_store;
  std::shared_ptr<SequencerPlacement> sequencer_placement;
  std::unique_ptr<RebuildingCoordinator> rebuilding_coordinator;
  std::unique_ptr<EventLogStateMachine> event_log;
  std::unique_ptr<RebuildingSupervisor> rebuilding_supervisor;
  std::shared_ptr<UnreleasedRecordDetector> unreleased_record_detector;
  std::unique_ptr<maintenance::ClusterMaintenanceStateMachine>
      cluster_maintenance_state_machine;

  shutdown_server(admin_handle,
                  connection_listener,
                  gossip_listener,
                  ssl_connection_listener,
                  server_to_server_listener,
                  connection_listener_loop,
                  gossip_listener_loop,
                  ssl_connection_listener_loop,
                  server_to_server_listener_loop,
                  logstore_monitor,
                  processor,
                  storage_thread_pool,
                  sharded_store,
                  sequencer_placement,
                  rebuilding_coordinator,
                  event_log,
                  rebuilding_supervisor,
                  unreleased_record_detector,
                  cluster_maintenance_state_machine,
                  false);
}
}} // namespace facebook::logdevice
