/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <limits>
#include <memory>

#include <folly/IntrusiveList.h>

#include "logdevice/common/BWAvailableCallback.h"
#include "logdevice/common/PriorityMap.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 * An Envelope is a control block that manages sending a Message via a
 * Connection for delivery to a LogDevice node or to a client. It is created
 * and owned by the Connection, but is also known to the Connection's associated
 * FlowGroup (within the Sender) until the Envelope is released for
 * transmission after any delay imposed by traffic shaping.
 *
 * An Envelope cannot be shared among several Connections, and the Connection is
 * responsible for deleting it. However, since the Envelope is known to
 * the FlowGroup before the Envelope is released, any Envelope waiting for
 * release must be removed from the FlowGroup before the Envelope is
 * destroyed.
 */

// see Envelope::pos_;
typedef uint64_t message_pos_t;

class Connection;

class Envelope : public BWAvailableCallback {
 public:
  explicit Envelope(Connection& conn, std::unique_ptr<Message> msg)
      : conn_(conn),
        msg_(std::move(msg)),
        drain_pos_(~0),
        birth_time_(std::chrono::steady_clock::now()),
        cost_(msg_->size()) {}

  // Used to track an Envelope on various queued in the Connection as
  // an Envelope is transmitted.
  folly::IntrusiveListHook links_;

  // BWAailableCallback Implementation.
  void operator()(FlowGroup& fg, std::mutex& flow_meters_mutex) override;

  Priority priority() const {
    return msg_->priority();
  }

  Connection& conn() const {
    return conn_;
  }
  size_t cost() const {
    return cost_;
  }

  const Message& message() const {
    return *msg_;
  }
  Message& message() {
    return *msg_;
  }
  bool haveMessage() {
    return static_cast<bool>(msg_);
  }

  std::unique_ptr<Message>&& moveMessage() {
    return std::move(msg_);
  }

  // Microseconds since the creation of this envelope.
  int64_t age() const {
    return usec_since(birth_time_);
  }

  // Microseconds since queued for send.
  int64_t enqTime() const {
    return usec_since(enq_time_);
  }

  void enqTime(std::chrono::steady_clock::time_point time) {
    enq_time_ = time;
  }

  SteadyTimestamp birthTime() const {
    return birth_time_;
  }

  message_pos_t getDrainPos() const {
    return drain_pos_;
  }
  void setDrainPos(message_pos_t pos) {
    drain_pos_ = pos;
  }

 private:
  Connection& conn_;
  std::unique_ptr<Message> msg_;
  std::chrono::steady_clock::time_point enq_time_;

  // offset of the first byte after this Envelope's Message in the logical
  // stream of bytes written into the output buffer of the Connection whose send
  // queue this Envelope is on. That's the logical byte stream counted from
  // the time the Connection was created.
  message_pos_t drain_pos_;

  // When this envelope was created.
  const std::chrono::steady_clock::time_point birth_time_;

  // Size in bytes charged against buffer limits while queued.
  // May be different from final serialized size (e.g. if
  // cancelled or protocol version changes).
  const size_t cost_;
};

}} // namespace facebook::logdevice
