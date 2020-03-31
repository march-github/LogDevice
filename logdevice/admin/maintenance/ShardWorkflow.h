/**
 *  Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 **/
#pragma once

#include <folly/container/F14Set.h>
#include <folly/futures/Future.h>

#include "logdevice/admin/maintenance/EventLogWriter.h"
#include "logdevice/admin/maintenance/types.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/membership/StorageState.h"
#include "logdevice/common/membership/StorageStateTransitions.h"

namespace facebook { namespace logdevice {
class EventLogRecord;
}} // namespace facebook::logdevice

namespace facebook { namespace logdevice { namespace maintenance {
/**
 * A ShardWorkflow is a state machine that tracks state
 * transitions of a shard.
 */
class ShardWorkflow {
 public:
  explicit ShardWorkflow(ShardID shard, const EventLogWriter* event_log_writer)
      : shard_(shard), event_log_writer_(event_log_writer) {
    created_at_ = SystemTimestamp::now();
    last_updated_at_ = created_at_;
  }

  // moveable.
  ShardWorkflow(ShardWorkflow&& /* unused */) = default;
  ShardWorkflow& operator=(ShardWorkflow&& wf) {
    return *this;
  }

  // non-copyable.
  ShardWorkflow(const ShardWorkflow& /* unused */) = delete;
  ShardWorkflow& operator=(const ShardWorkflow& /* unused */) = delete;

  virtual ~ShardWorkflow() {}

  /**
   * Computes the new MaintenanceStatus based on the parameters
   * passed
   *
   * @param shard_state           The membership ShardState in NC
   * @param excluded_from_nodeset Whether currently this shard is
   *                              excluded from nodesets or not.
   * @param data_health           ShardDataHealth for the shard
   * @param rebuilding_mode       RebuildingMode for the shard
   * @param is_draining           Is drain flag set in event log
   * @param is_non_authoritative  Is current rebuilding non authoritative.
   * @param node_gossip_state The gossip state of the node for this shard
   *
   * @return folly::SemiFuture<MaintenanceStatus> A SemiFuture out of
   *      MaintenanceStatus. Promise is fulfiled immediately if there
   *      is no record to be written to event log. Otherwise it will be
   *      fulfiled once the record is written to event log in the context
   *      of the thread doing the write to EventLog
   */
  folly::SemiFuture<thrift::MaintenanceStatus>
  run(const membership::ShardState& shard_state,
      bool excluded_from_nodeset,
      ShardDataHealth data_health,
      RebuildingMode rebuilding_mode,
      bool is_draining,
      bool is_non_authoritative,
      ClusterStateNodeState node_gossip_state);

  // Returns the ShardID for this workflow
  ShardID getShardID() const;

  // Adds state to target_op_state_
  void addTargetOpState(folly::F14FastSet<ShardOperationalState> state);

  // Sets allow_passive_drain to `allow`.
  void isPassiveDrainAllowed(bool allow);

  // Sets skip_safety_check_ to value of `skip`
  void shouldSkipSafetyCheck(bool skip);

  // Sets the RebuildingMode for the maintenance
  void rebuildInRestoreMode(bool is_restore);

  // If filter_relocate_shards is true, FILTER_RELOCATE_SHARDS will be
  // added to SHARD_NEEDS_REBUILD event
  void rebuildingFilterRelocateShards(bool filter_relocate_shards);

  // Returns the target_op_state_
  folly::F14FastSet<ShardOperationalState> getTargetOpStates() const;

  // Returns value of last_updated_at_
  SystemTimestamp getLastUpdatedTimestamp() const;

  // Returns value of created_at_;
  SystemTimestamp getCreationTimestamp() const;

  // Returns the StorageStateTransition that this workflow expects for this
  // shard in NodesConfiguration. This will be used
  // by the MaintenanceManager in NodesConfig update request.
  folly::Optional<membership::StorageStateTransition>
  getExpectedStorageStateTransition() const;

  // Returns value of allow_passive_drain_
  bool allowPassiveDrain() const;

  bool operator==(const ShardWorkflow& other) const {
    return shard_ == other.shard_ &&
        target_op_state_ == other.target_op_state_ &&
        allow_passive_drain_ == other.allow_passive_drain_ &&
        skip_safety_check_ == other.skip_safety_check_ &&
        restore_mode_rebuilding_ == other.restore_mode_rebuilding_ &&
        filter_relocate_shards_ == other.filter_relocate_shards_;
  }

 protected:
  // Method that is called when there is an event that needs to
  // be written to event log by this workflow
  virtual void writeToEventLog(
      std::unique_ptr<EventLogRecord> event,
      std::function<void(Status st, lsn_t lsn, const std::string& str)> cb)
      const;

 private:
  folly::F14FastSet<ShardOperationalState> target_op_state_;
  // The shard this workflow is for
  ShardID shard_;
  // Any even that needs to be written by this workflow
  // is written through this object
  const EventLogWriter* event_log_writer_;
  // StorageStateTransition to be requested
  // in the NodesConfiguration. Workflow will set this value
  // and MaintenanceManager will make use of it to request
  // the update in NodesConfiguration.
  folly::Optional<membership::StorageStateTransition>
      expected_storage_state_transition_{folly::none};
  // If safety checker determines that a drain is needed,
  // allow passive drain if reruired
  bool allow_passive_drain_{false};
  // If true, skip safety check for this workflow
  bool skip_safety_check_{false};
  // True if RebuildingMode requested by the maintenance is RESTORE.
  // Mainly set by internal maintenance request when a shard is down
  bool restore_mode_rebuilding_{false};
  // True if rebuilding needs to filter shards in relocate mode
  bool filter_relocate_shards_{false};
  // The EventLogRecord to write as determined by workflow.
  // nullptr if there isn't one to write
  std::unique_ptr<EventLogRecord> event_;
  // Latest MaintenanceStatus. Updated every time `run`
  // is called
  MaintenanceStatus status_;
  // The last StorageState as informed by MM for this shard
  // Gets updated every time `run` is called
  membership::StorageState current_storage_state_;
  // Whether we are currently being excluded from nodesets or not.
  bool is_excluded_from_nodeset_{false};
  // The last ShardDataHealth as informed by the MM for this
  // shard.
  // Gets updated every time `run` is called
  ShardDataHealth current_data_health_;
  // True if current rebuilding has drain flag set
  // in the event log
  bool current_is_draining_;
  // True if current rebuilding is non authoritative
  // Gets updated every time `run` is called
  bool current_rebuilding_is_non_authoritative_;
  // The last known gossip state for the node.
  ClusterStateNodeState gossip_state_;
  // The last RebuildingMode as informed by the MM for this
  // shard.
  // Gets updated every time `run` is called
  RebuildingMode current_rebuilding_mode_;
  // Last time the status_ was updated
  SystemTimestamp last_updated_at_;
  // Time when this workflow was created
  SystemTimestamp created_at_;
  // Updates the status_ with given value if
  // it differs from current value
  void updateStatus(MaintenanceStatus status);
  // Determines the next MaintenanceStatus based on
  // current storage state, shard data health and rebuilding mode
  void computeMaintenanceStatus();
  // Helper methods to compute the MaintenanceStatus for each of
  // the possible target states
  void computeMaintenanceStatusForDrain();
  void computeMaintenanceStatusForMayDisappear();
  void computeMaintenanceStatusForEnable();
  // Sets event_ to SHARD_NEEDS_REBUILD_Event with appropriate flags. If force
  // argument is true, a new event will be created irrespective of the
  // current_rebuilding_mode_ and current_is_draining_
  void createRebuildEventIfRequired(bool force = false);
  // Sets event_ to SHARD_ABORT_EVENT if this is a full shard
  // rebuilding based on current_data_health_ and current_rebuilding_mode_
  void createAbortEventIfRequired();
  // Returns true if status_ is AWAITING_NODES_CONFIG_TRANSITION and
  // it has been atleast
  // 2 * nodes-configuration-manager-intermediary-shard-state-timeout since
  // last time the status_ was updated
  virtual bool isNcTransitionStuck() const;
};

}}} // namespace facebook::logdevice::maintenance
