/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/Connection.h"

#include <algorithm>
#include <errno.h>
#include <functional>
#include <memory>

#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <folly/io/SocketOptionMap.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <openssl/err.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "event2/bufferevent.h"
#include "event2/bufferevent_ssl.h"
#include "event2/event.h"
#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/BWAvailableCallback.h"
#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/EventHandler.h"
#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/ProtocolHandler.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/SocketDependencies.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/network/MessageReader.h"
#include "logdevice/common/network/SocketAdapter.h"
#include "logdevice/common/network/SocketConnectCallback.h"
#include "logdevice/common/protocol/Compatibility.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageTypeNames.h"
#include "logdevice/common/protocol/ProtocolHeader.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"

#ifdef __linux__
#ifndef TCP_USER_TIMEOUT
#define TCP_USER_TIMEOUT 18
#endif
#endif

namespace facebook { namespace logdevice {
using folly::SSLContext;
using namespace std::placeholders;

class SocketImpl {
 public:
  SocketImpl() {}

  // an intrusive list of callback functors to call when the socket closes
  folly::IntrusiveList<SocketCallback, &SocketCallback::listHook_> on_close_;

  // an intrusive list of the pending bandwidth available callbacks for
  // state machines waiting to run on this socket. These callbacks must
  // be cleaned up when the socket is closed.
  folly::IntrusiveList<BWAvailableCallback, &BWAvailableCallback::links_>
      pending_bw_cbs_;
};

static std::chrono::milliseconds
getTimeDiff(std::chrono::steady_clock::time_point& start_time) {
  auto diff = std::chrono::steady_clock::now() - start_time;
  return std::chrono::duration_cast<std::chrono::milliseconds>(diff);
}

Connection::Connection(std::unique_ptr<SocketDependencies>& deps,
                       Address peer_name,
                       const Sockaddr& peer_sockaddr,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group)
    : peer_name_(peer_name),
      peer_sockaddr_(peer_sockaddr),
      conn_description_(peer_name.toString() + "(" +
                        (peer_sockaddr_.valid() ? peer_sockaddr_.toString()
                                                : std::string("UNKNOWN")) +
                        ")"),
      flow_group_(flow_group),
      type_(type),
      socket_ref_holder_(std::make_shared<bool>(true), this),
      impl_(new SocketImpl),
      deps_(std::move(deps)),
      next_pos_(0),
      drain_pos_(0),
      bev_(nullptr),
      connected_(false),
      handshaken_(false),
      proto_(getSettings().max_protocol),
      our_name_at_peer_(ClientID::INVALID),
      outbuf_overflow_(getSettings().outbuf_overflow_kb * 1024),
      outbufs_min_budget_(getSettings().outbuf_socket_min_kb * 1024),
      read_more_(deps_->getEvBase()),
      connect_timeout_event_(deps_->getEvBase()),
      retries_so_far_(0),
      handshake_timeout_event_(deps_->getEvBase()),
      first_attempt_(true),
      tcp_sndbuf_cache_({128 * 1024, std::chrono::steady_clock::now()}),
      tcp_rcvbuf_size_(128 * 1024),
      close_reason_(E::UNKNOWN),
      num_messages_sent_(0),
      num_messages_received_(0),
      num_bytes_received_(0),
      deferred_event_queue_event_(deps_->getEvBase()),
      end_stream_rewind_event_(deps_->getEvBase()),
      buffered_output_flush_event_(deps_->getEvBase()),
      legacy_connection_(deps_->attachedToLegacyEventBase()),
      retry_receipt_of_message_(deps_->getEvBase()),
      sched_write_chain_(deps_->getEvBase()) {
  conntype_ = conntype;

  if (!peer_sockaddr.valid()) {
    ld_check(!peer_name.isClientAddress());
    if (conntype_ == ConnectionType::SSL) {
      err = E::NOSSLCONFIG;
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      2,
                      "Recipient %s is not configured for SSL connections.",
                      peer_name_.toString().c_str());
    } else {
      err = E::NOTINCONFIG;
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      2,
                      "Invalid address for %s.",
                      peer_name_.toString().c_str());
    }
    throw ConstructorFailed();
  }

  read_more_.attachCallback([this] {
    bumpEventHandersCalled();
    onBytesAvailable(false /*fresh*/);
    bumpEventHandlersCompleted();
  });
  connect_timeout_event_.attachCallback([this] {
    bumpEventHandersCalled();
    onConnectAttemptTimeout();
    bumpEventHandlersCompleted();
  });
  handshake_timeout_event_.attachCallback([this] {
    bumpEventHandersCalled();
    onHandshakeTimeout();
    bumpEventHandlersCompleted();
  });
  deferred_event_queue_event_.attachCallback([this] {
    bumpEventHandersCalled();
    processDeferredEventQueue();
    bumpEventHandlersCompleted();
  });
  end_stream_rewind_event_.attachCallback([this] {
    bumpEventHandersCalled();
    endStreamRewind();
    bumpEventHandlersCompleted();
  });

  int rv = end_stream_rewind_event_.setPriority(EventLoop::PRIORITY_HIGH);
  if (rv != 0) {
    err = E::INTERNAL;
    throw ConstructorFailed();
  }

  buffered_output_flush_event_.attachCallback([this] {
    bumpEventHandersCalled();
    flushBufferedOutput();
    bumpEventHandlersCompleted();
  });
}

Connection::Connection(NodeID server_name,
                       SocketType socket_type,
                       ConnectionType connection_type,
                       PeerType peer_type,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps)
    : Connection(deps,
                 Address(server_name),
                 deps->getNodeSockaddr(server_name,
                                       socket_type,
                                       connection_type,
                                       peer_type),
                 socket_type,
                 connection_type,
                 flow_group) {}

Connection::Connection(NodeID server_name,
                       SocketType socket_type,
                       ConnectionType connection_type,
                       PeerType peer_type,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps,
                       std::unique_ptr<SocketAdapter> sock_adapter)
    : Connection(server_name,
                 socket_type,
                 connection_type,
                 peer_type,
                 flow_group,
                 std::move(deps)) {
  ld_check(!legacy_connection_);
  proto_handler_ = std::make_shared<ProtocolHandler>(
      this, std::move(sock_adapter), conn_description_, deps_->getEvBase());
  sock_write_cb_ = SocketWriteCallback(proto_handler_.get());
  proto_handler_->getSentEvent()->attachCallback([this] { drainSendQueue(); });
}

Connection::Connection(int fd,
                       ClientID client_name,
                       const Sockaddr& client_addr,
                       ResourceBudget::Token conn_token,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps)
    : Connection(deps,
                 Address(client_name),
                 client_addr,
                 type,
                 conntype,
                 flow_group) {
  ld_check(fd >= 0);
  ld_check(client_name.valid());
  ld_check(client_addr.valid());

  // note that caller (Sender.addClient()) does not close(fd) on error.
  // If you add code here that throws ConstructorFailed you must close(fd)!

  if (legacy_connection_) {
    bev_ = newBufferevent(fd,
                          client_addr.family(),
                          &tcp_sndbuf_cache_.size,
                          &tcp_rcvbuf_size_,
                          // This is only used if conntype_ == SSL, tells
                          // libevent we are in a server context
                          BUFFEREVENT_SSL_ACCEPTING,
                          getSettings().client_dscp_default);
    if (!bev_) {
      throw ConstructorFailed(); // err is already set
    }
  }
  conn_closed_ = std::make_shared<std::atomic<bool>>(false);
  conn_incoming_token_ = std::move(conn_token);

  addHandshakeTimeoutEvent();
  expectProtocolHeader();

  if (isSSL()) {
    expecting_ssl_handshake_ = true;
  }
  connected_ = true;
  peer_shuttingdown_ = false;
  fd_ = fd;

  STAT_INCR(deps_->getStats(), num_connections);
  STAT_DECR(deps_->getStats(), num_backlog_connections);
  if (isSSL()) {
    STAT_INCR(deps_->getStats(), num_ssl_connections);
  }
}

Connection::Connection(int fd,
                       ClientID client_name,
                       const Sockaddr& client_addr,
                       ResourceBudget::Token conn_token,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps,
                       std::unique_ptr<SocketAdapter> sock_adapter)
    : Connection(fd,
                 client_name,
                 client_addr,
                 std::move(conn_token),
                 type,
                 conntype,
                 flow_group,
                 std::move(deps)) {
  ld_check(!legacy_connection_);
  proto_handler_ = std::make_shared<ProtocolHandler>(
      this, std::move(sock_adapter), conn_description_, getDeps()->getEvBase());
  sock_write_cb_ = SocketWriteCallback(proto_handler_.get());
  proto_handler_->getSentEvent()->attachCallback([this] { drainSendQueue(); });
  // Set the read callback.
  read_cb_.reset(new MessageReader(*proto_handler_, proto_));
  proto_handler_->sock()->setReadCB(read_cb_.get());
}

void Connection::onBufferedOutputWrite(struct evbuffer* buffer,
                                       const struct evbuffer_cb_info* info,
                                       void* arg) {
  Connection* self = reinterpret_cast<Connection*>(arg);

  ld_check(self);
  ld_check(!self->isClosed());
  ld_check(self->buffered_output_);
  ld_check(buffer == self->buffered_output_);

  if (info->n_added) {
    self->buffered_output_flush_event_.scheduleTimeout(0);
  }
}

void Connection::flushBufferedOutput() {
  ld_check(buffered_output_);
  ld_check(!isClosed());
  // Moving buffer chains into bev's output
  int rv = LD_EV(evbuffer_add_buffer)(deps_->getOutput(bev_), buffered_output_);
  if (rv != 0) {
    ld_error("evbuffer_add_buffer() failed. error %d", rv);
    err = E::NOMEM;
    close(E::NOMEM);
  }

  // the buffered_output_ size might not be 0 because of minor size limit
  // differences with the actual outbuf. We also have to check if we are still
  // connected here, because the socket might have been closed above, or if we
  // flushed the last bytes (see flushOutputandClose())
  if (connected_ && LD_EV(evbuffer_get_length)(buffered_output_) != 0) {
    buffered_output_flush_event_.scheduleTimeout(0);
  }
}

void Connection::onBufferedOutputTimerEvent(void* instance, short) {
  auto self = reinterpret_cast<Connection*>(instance);
  ld_check(self);
  self->flushBufferedOutput();
}

Connection::~Connection() {
  auto g = folly::makeGuard(deps_->setupContextGuard());
  ld_debug("Destroying Socket %s", conn_description_.c_str());
  close(E::SHUTDOWN);
}

struct bufferevent* Connection::newBufferevent(int sfd,
                                               sa_family_t sa_family,
                                               size_t* sndbuf_size_out,
                                               size_t* rcvbuf_size_out,
                                               bufferevent_ssl_state ssl_state,
                                               const uint8_t default_dcsp) {
  int rv;

  ld_check(sa_family == AF_INET || sa_family == AF_INET6 ||
           sa_family == AF_UNIX);

  if (sfd < 0) {
    sfd = socket(sa_family, SOCK_STREAM, 0);
    if (sfd < 0) {
      ld_error("socket() failed. errno=%d (%s)", errno, strerror(errno));
      switch (errno) {
        case EMFILE:
        case ENFILE:
          err = E::SYSLIMIT;
          break;
        case ENOBUFS:
        case ENOMEM:
          err = E::NOMEM;
          break;
        default:
          err = E::INTERNAL;
      }
      return nullptr;
    }
  }

  rv = deps_->evUtilMakeSocketNonBlocking(sfd);
  if (rv != 0) { // unlikely
    ld_error("Failed to make fd %d non-blocking. errno=%d (%s)",
             sfd,
             errno,
             strerror(errno));
    ::close(sfd);
    err = E::INTERNAL;
    return nullptr;
  }

  int tcp_sndbuf_size = 0, tcp_rcvbuf_size = 0;

  deps_->configureSocket(!peer_sockaddr_.isUnixAddress(),
                         sfd,
                         &tcp_sndbuf_size,
                         &tcp_rcvbuf_size,
                         sa_family,
                         default_dcsp);

  if (isSSL()) {
    ld_check(!ssl_context_);
    ssl_context_ = deps_->getSSLContext();
  }

  struct bufferevent* bev = deps_->buffereventSocketNew(
      sfd, BEV_OPT_CLOSE_ON_FREE, isSSL(), ssl_state, ssl_context_.get());
  if (!bev) { // unlikely
    ld_error("bufferevent_socket_new() failed. errno=%d (%s)",
             errno,
             strerror(errno));
    err = E::NOMEM;
    ::close(sfd);
    return nullptr;
  }

  struct evbuffer* outbuf = deps_->getOutput(bev);
  ld_check(outbuf);

  struct evbuffer_cb_entry* outbuf_cbe = LD_EV(evbuffer_add_cb)(
      outbuf,
      &EvBufferEventHandler<Connection::bytesSentCallback>,
      (void*)this);

  if (!outbuf_cbe) { // unlikely
    ld_error("evbuffer_add_cb() failed. errno=%d (%s)", errno, strerror(errno));
    err = E::NOMEM;
    ::close(sfd);
    return nullptr;
  }

  // At this point, we are convinced the socket we are using is legit.
  fd_ = sfd;

  if (tcp_sndbuf_size > 0) {
    deps_->buffereventSetMaxSingleWrite(bev, tcp_sndbuf_size);
    if (sndbuf_size_out) {
      *sndbuf_size_out = tcp_sndbuf_size;
    }
  }

  if (tcp_rcvbuf_size > 0) {
    deps_->buffereventSetMaxSingleRead(bev, tcp_rcvbuf_size);
    if (rcvbuf_size_out) {
      *rcvbuf_size_out = tcp_rcvbuf_size;
    }
  }

  deps_->buffereventSetCb(bev,
                          BufferEventHandler<Connection::dataReadCallback>,
                          nullptr,
                          BufferEventHandler<Connection::eventCallback>,
                          (void*)this);

  if (isSSL()) {
    // The buffer may already exist if we're making another attempt at a
    // connection
    if (!buffered_output_) {
      // creating an evbuffer that would batch up SSL writes
      buffered_output_ = LD_EV(evbuffer_new)();
      LD_EV(evbuffer_add_cb)
      (buffered_output_,
       &EvBufferEventHandler<Connection::onBufferedOutputWrite>,
       (void*)this);
    }
  } else {
    buffered_output_ = nullptr;
  }

  deps_->buffereventEnable(bev, EV_READ | EV_WRITE);

  return bev;
}

int Connection::preConnectAttempt() {
  if (peer_name_.isClientAddress()) {
    if (!isClosed()) {
      ld_check(connected_);
      err = E::ISCONN;
    } else {
      err = E::UNREACHABLE;
    }
    return -1;
  }

  // it's a server socket

  if (!isClosed()) {
    err = connected_ ? E::ISCONN : E::ALREADY;
    return -1;
  }

  // it's an unconnected server socket

  ld_check(!connected_);
  ld_check(pendingq_.empty());
  ld_check(serializeq_.empty());
  ld_check(sendq_.empty());
  ld_check(getBytesPending() == 0);
  ld_check(connect_throttle_);

  if (connect_throttle_ && !connect_throttle_->mayConnect()) {
    err = E::DISABLED;
    return -1;
  }
  return 0;
}

static folly::SocketOptionMap
getDefaultSocketOptions(const folly::SocketAddress& sock_addr,
                        const Settings& settings) {
  folly::SocketOptionMap options;
  sa_family_t sa_family = sock_addr.getFamily();
  bool is_tcp = !(sa_family == AF_UNIX);

  using OptionKey = folly::SocketOptionKey;

  // Set send buffer size
  int sndbuf_size = settings.tcp_sendbuf_kb * 1024;
  options.emplace(OptionKey{SOL_SOCKET, SO_SNDBUF}, sndbuf_size);

  // Set receive buffer size.
  int rcvbuf_size = settings.tcp_rcvbuf_kb * 1024;
  options.emplace(OptionKey{SOL_SOCKET, SO_RCVBUF}, rcvbuf_size);

  if (is_tcp) {
    if (!settings.nagle) {
      options.emplace(OptionKey{IPPROTO_TCP, TCP_NODELAY}, 1);
    }
  }

  bool keep_alive = settings.use_tcp_keep_alive;
  if (is_tcp && keep_alive) {
    int keep_alive_time = settings.tcp_keep_alive_time;
    int keep_alive_intvl = settings.tcp_keep_alive_intvl;
    int keep_alive_probes = settings.tcp_keep_alive_probes;
    options.emplace(OptionKey{SOL_SOCKET, SO_KEEPALIVE}, keep_alive);
    if (keep_alive_time > 0) {
      options.emplace(OptionKey{SOL_TCP, TCP_KEEPIDLE}, keep_alive_time);
    }
    if (keep_alive_intvl > 0) {
      options.emplace(OptionKey{SOL_TCP, TCP_KEEPINTVL}, keep_alive_intvl);
    }
    if (keep_alive_probes > 0) {
      options.emplace(OptionKey{SOL_TCP, TCP_KEEPCNT}, keep_alive_probes);
    }
  }

#ifdef __linux__
  if (is_tcp) {
    int tcp_user_timeout = settings.tcp_user_timeout;
    if (tcp_user_timeout >= 0) {
      options.emplace(OptionKey{SOL_TCP, TCP_USER_TIMEOUT}, tcp_user_timeout);
    }
  }
#endif

  const uint8_t default_dscp = settings.server ? settings.server_dscp_default
                                               : settings.client_dscp_default;
  const int diff_svcs = default_dscp << 2;
  switch (sa_family) {
    case AF_INET: {
      options.emplace(OptionKey{IPPROTO_IP, IP_TOS}, diff_svcs);
      break;
    }
    case AF_INET6: {
      options.emplace(OptionKey{IPPROTO_IPV6, IPV6_TCLASS}, diff_svcs);
      break;
    }
    default:
      break;
  }
  return options;
}

folly::Future<Status> Connection::asyncConnect() {
  std::chrono::milliseconds timeout = getSettings().connect_timeout;
  size_t max_retries = getSettings().connection_retries;
  auto connect_timeout_retry_multiplier =
      getSettings().connect_timeout_retry_multiplier;
  folly::SocketOptionMap options(getDefaultSocketOptions(
      peer_sockaddr_.getSocketAddress(), getSettings()));

  for (size_t retry_count = 1; retry_count < max_retries; ++retry_count) {
    timeout += std::chrono::duration_cast<std::chrono::milliseconds>(
        getSettings().connect_timeout *
        pow(connect_timeout_retry_multiplier, retry_count));
  }

  auto connect_cb = std::make_unique<SocketConnectCallback>();

  /* TODO(gauresh) : Go to worker in future. using unsafe future for now.
  auto executor = worker_ != nullptr ? worker_->getExecutor()
                                     : &folly::InlineExecutor::instance();
                                     */
  auto fut = connect_cb->getConnectStatus().toUnsafeFuture();

  proto_handler_->sock()->connect(connect_cb.get(),
                                  peer_sockaddr_.getSocketAddress(),
                                  timeout.count(),
                                  options);

  auto dispatch_status = [this](const folly::AsyncSocketException& ex) mutable {
    err = ProtocolHandler::translateToLogDeviceStatus(ex);
    if (err != E::ISCONN) {
      proto_handler_->notifyErrorOnSocket(ex);
    }
    if (err == E::TIMEDOUT) {
      STAT_INCR(deps_->getStats(), connection_timeouts);
    }
    return err;
  };
  if (fut.isReady()) {
    folly::AsyncSocketException ex(std::move(fut.value()));
    return folly::makeFuture<Status>(dispatch_status(ex));
  }

  return std::move(fut).thenValue(
      [connect_cb = std::move(connect_cb),
       dispatch_status = std::move(dispatch_status)](
          const folly::AsyncSocketException& ex) mutable {
        return dispatch_status(ex);
      });
}

int Connection::connect() {
  int rv = preConnectAttempt();
  if (rv != 0) {
    return rv;
  }

  if (legacy_connection_) {
    retries_so_far_ = 0;

    rv = doConnectAttempt();
    if (rv != 0) {
      if (!isClosed()) {
        STAT_INCR(deps_->getStats(), num_connections);
        close(err);
      }
      return -1; // err set by doConnectAttempt
    }
    if (isSSL()) {
      ld_check(bev_);
      ld_assert(bufferevent_get_openssl_error(bev_) == 0);
    }

    next_pos_ = 0;
    drain_pos_ = 0;
    health_stats_.clear();
    sendHello(); // queue up HELLO, to be sent when we connect

    RATELIMIT_DEBUG(std::chrono::seconds(1),
                    10,
                    "Connected %s socket via %s channel to %s",
                    getSockType() == SocketType::DATA ? "DATA" : "GOSSIP",
                    getConnType() == ConnectionType::SSL ? "SSL" : "PLAIN",
                    peerSockaddr().toString().c_str());
  } else {
    auto fut = asyncConnect();

    fd_ = proto_handler_->sock()->getNetworkSocket().toFd();
    conn_closed_ = std::make_shared<std::atomic<bool>>(false);
    next_pos_ = 0;
    drain_pos_ = 0;

    if (good()) {
      // enqueue hello message into the socket.
      sendHello();
    }

    auto complete_connection = [this](Status st) {
      auto g = folly::makeGuard(getDeps()->setupContextGuard());
      if (st == E::ISCONN) {
        transitionToConnected();
        read_cb_.reset(new MessageReader(*proto_handler_, proto_));
        proto_handler_->sock()->setReadCB(read_cb_.get());
      }
    };

    if (!fut.isReady()) {
      std::move(fut).thenValue(
          [connect_completion = std::move(complete_connection)](Status st) {
            connect_completion(st);
          });
    } else {
      complete_connection(std::move(fut.value()));
    }

    RATELIMIT_DEBUG(
        std::chrono::seconds(1),
        10,
        "Connected %s socket via %s channel to %s, immediate_connect "
        "%d, immediate_fail %d",
        getSockType() == SocketType::DATA ? "DATA" : "GOSSIP",
        getConnType() == ConnectionType::SSL ? "SSL" : "PLAIN",
        peerSockaddr().toString().c_str(),
        connected_,
        !proto_handler_->good());
  }

  STAT_INCR(deps_->getStats(), num_connections);
  if (isSSL()) {
    STAT_INCR(deps_->getStats(), num_ssl_connections);
  }

  return 0;
}

int Connection::doConnectAttempt() {
  const uint8_t default_dscp = getSettings().server
      ? getSettings().server_dscp_default
      : getSettings().client_dscp_default;

  ld_check(!connected_);
  ld_check(!bev_);
  bev_ = newBufferevent(-1,
                        peer_sockaddr_.family(),
                        &tcp_sndbuf_cache_.size,
                        &tcp_rcvbuf_size_,
                        // This is only used if conntype_ == SSL, tells libevent
                        // we are in a client context
                        BUFFEREVENT_SSL_CONNECTING,
                        default_dscp);

  if (!bev_) {
    return -1; // err is already set
  }
  conn_closed_ = std::make_shared<std::atomic<bool>>(false);
  expectProtocolHeader();

  struct sockaddr_storage ss;
  int len = peer_sockaddr_.toStructSockaddr(&ss);
  if (len == -1) {
    // This can only fail if node->address is an invalid Sockaddr.  Since the
    // address comes from Configuration, it must have been validated already.
    err = E::INTERNAL;
    ld_check(false);
    return -1;
  }
  const int rv = deps_->buffereventSocketConnect(
      bev_, reinterpret_cast<struct sockaddr*>(&ss), len);

  if (rv != 0) {
    if (isSSL() && bev_) {
      unsigned long ssl_err = 0;
      char ssl_err_string[120];
      while ((ssl_err = bufferevent_get_openssl_error(bev_))) {
        ERR_error_string_n(ssl_err, ssl_err_string, sizeof(ssl_err_string));
        RATELIMIT_ERROR(
            std::chrono::seconds(10), 10, "SSL error: %s", ssl_err_string);
      }
    }

    ld_error("Failed to initiate connection to %s errno=%d (%s)",
             conn_description_.c_str(),
             errno,
             strerror(errno));
    switch (errno) {
      case ENOMEM:
        err = E::NOMEM;
        break;
      case ENETUNREACH:
      case ENOENT: // for unix domain sockets.
        err = E::UNROUTABLE;
        break;
      case EAGAIN:
        err = E::SYSLIMIT; // out of ephemeral ports
        break;
      case ECONNREFUSED: // TODO: verify
      case EADDRINUSE:   // TODO: verify
      default:
        // Linux does not report ECONNREFUSED for non-blocking TCP
        // sockets even if connecting over loopback. Other errno values
        // can only be explained by an internal error such as memory
        // corruption or a bug in libevent.
        err = E::INTERNAL;
        break;
    }

    return -1;
  }
  if (isSSL()) {
    ld_assert(bufferevent_get_openssl_error(bev_) == 0);
  }

  // Start a timer for this connection attempt. When the timer triggers, this
  // function will be called again until we reach the maximum amount of
  // connection retries.
  addConnectAttemptTimeoutEvent();
  return 0;
}

size_t Connection::getTotalOutbufLength() {
  auto pending_bytes = LD_EV(evbuffer_get_length)(deps_->getOutput(bev_));
  if (buffered_output_) {
    pending_bytes += LD_EV(evbuffer_get_length)(buffered_output_);
  }
  return pending_bytes;
}

void Connection::onOutputEmpty(struct bufferevent*, void* arg, short) {
  Connection* self = reinterpret_cast<Connection*>(arg);
  ld_check(self);
  // Write watermark has been set to zero so the output buffer should be
  // empty when this callback gets called, but we could still have bytes
  // pending in buffered_output_
  auto pending_bytes = self->getTotalOutbufLength();
  if (pending_bytes == 0) {
    self->close(self->close_reason_);
  } else {
    ld_info("Not closing socket because %lu bytes are still pending",
            pending_bytes);
  }
}

void Connection::flushOutputAndClose(Status reason) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  if (isClosed()) {
    return;
  }
  auto pending_bytes =
      legacy_connection_ ? getTotalOutbufLength() : getBufferedBytesSize();

  if (pending_bytes == 0) {
    close(reason);
    return;
  }

  ld_spew("Flushing %lu bytes of output before closing connection to %s",
          pending_bytes,
          conn_description_.c_str());

  close_reason_ = reason;

  if (legacy_connection_) {
    // - Remove the read callback as we are not processing any more message
    //   since we are about to close the connection.
    // - Set up the write callback and the low write watermark to 0 so that the
    //   callback will be called when the output buffer is flushed and will
    //   close the connection using close_reason_ for the error code.
    deps_->buffereventSetWatermark(bev_, EV_WRITE, 0, 0);
    deps_->buffereventSetCb(bev_,
                            nullptr,
                            BufferEventHandler<Connection::onOutputEmpty>,
                            BufferEventHandler<Connection::eventCallback>,
                            (void*)this);
  } else {
    // For new sockets, set the readcallback to nullptr as we know that socket
    // is getting closed.
    proto_handler_->sock()->setReadCB(nullptr);
  }
}

void Connection::onBytesAvailable(bool fresh) {
  // process up to this many messages
  unsigned process_max = getSettings().incoming_messages_max_per_socket;

  size_t bytes_cached = msg_pending_processing_
      ? msg_pending_processing_->computeChainDataLength()
      : 0;
  size_t available =
      LD_EV(evbuffer_get_length)(deps_->getInput(bev_)) + bytes_cached;

  // if this function was called by bev_ in response to "input buffer
  // length is above low watermark" event, we must have at least as
  // many bytes available as Socket is expecting. Otherwise the
  // function was called by read_more_ event, which may run after
  // "bev_ is readable" if TCP socket becomes readable after
  // read_more_ was activated. If that happens this callback may find
  // fewer bytes in bev_'s input buffer than dataReadCallback() expects.
  ld_assert(!fresh || available >= bytesExpected());
  auto start_time = std::chrono::steady_clock::now();
  unsigned i = 0;
  STAT_INCR(deps_->getStats(), sock_read_events);
  for (;; i++) {
    if (available >= bytesExpected()) {
      // it's i/2 because we need 2 calls : one for the protocol header, the
      // other for message
      if (i / 2 < process_max) {
        struct evbuffer* inbuf = deps_->getInput(bev_);
        int rv = 0;
        if (expectingProtocolHeader()) {
          // We always have space for header.
          ld_check(!msg_pending_processing_);
          rv = readMessageHeader(inbuf);
          if (rv == 0) {
            expectMessageBody();
          }
        } else {
          auto expected_bytes = bytesExpected();
          size_t read_bytes = 0;
          if (!msg_pending_processing_) {
            msg_pending_processing_ = folly::IOBuf::create(expected_bytes);
            rv = LD_EV(evbuffer_remove)(
                inbuf,
                static_cast<void*>(msg_pending_processing_->writableData()),
                expected_bytes);
            if (rv > 0) {
              read_bytes = rv;
              msg_pending_processing_->append(expected_bytes);
            } else {
              err = E::INTERNAL;
            }
          } else {
            ld_check(msg_pending_processing_ && bytes_cached > 0);
            read_bytes = bytes_cached;
          }
          if (read_bytes > 0) {
            ld_check_eq(read_bytes, expected_bytes);
            rv = dispatchMessageBody(
                recv_message_ph_, msg_pending_processing_->clone());
            if (rv == 0) {
              msg_pending_processing_.reset();
              expectProtocolHeader();
            }
          }
        }
        if (rv != 0) {
          if (!peer_name_.isClientAddress()) {
            RATELIMIT_ERROR(std::chrono::seconds(10),
                            10,
                            "reading message failed with %s from %s.",
                            error_name(err),
                            conn_description_.c_str());
          }
          if (err == E::NOBUFS) {
            STAT_INCR(deps_->getStats(), sock_read_event_nobufs);
            ld_check(msg_pending_processing_);
            // Ran out of space to enqueue message into worker. Try again.
            read_more_.scheduleTimeout(0);
            break;
          }
          if ((err == E::PROTONOSUPPORT || err == E::INVALID_CLUSTER ||
               err == E::ACCESS || err == E::DESTINATION_MISMATCH) &&
              isHELLOMessage(recv_message_ph_.type)) {
            // Make sure the ACK message with E::PROTONOSUPPORT, E::ACCESS,
            // E::DESTINATION_MISMATCH or E::INVALID_CLUSTER error is sent to
            // the client before the socket is closed.
            flushOutputAndClose(err);
          } else {
            close(err);
          }
          break;
        }
      } else {
        // We reached the limit of how many messages we are allowed to
        // process before returning control to libevent. schedule
        // read_more_ to fire in the next iteration of event loop and
        // return control to libevent so that we can run other events
        read_more_.scheduleTimeout(0);
        break;
      }
    } else {
      read_more_.cancelTimeout();
      break;
    }

    ld_check(!isClosed());
    ld_check(!msg_pending_processing_);
    available = LD_EV(evbuffer_get_length)(deps_->getInput(bev_));
  }

  STAT_ADD(deps_->getStats(), sock_num_messages_read, i);
  auto total_time = getTimeDiff(start_time);
  STAT_ADD(
      deps_->getStats(), sock_time_spent_reading_message, total_time.count());
}

void Connection::dataReadCallback(struct bufferevent* bev, void* arg, short) {
  Connection* self = reinterpret_cast<Connection*>(arg);

  ld_check(self);
  ld_check(bev == self->bev_);

  self->onBytesAvailable(/*fresh=*/true);
}

void Connection::readMoreCallback(void* arg, short what) {
  Connection* self = reinterpret_cast<Connection*>(arg);
  ld_check(what & EV_TIMEOUT);
  ld_check(!self->isClosed());
  ld_spew(
      "Socket %s remains above low watermark", self->conn_description_.c_str());
  self->onBytesAvailable(/*fresh=*/false);
}

size_t Connection::bytesExpected() {
  size_t protohdr_bytes =
      ProtocolHeader::bytesNeeded(recv_message_ph_.type, proto_);

  if (expectingProtocolHeader()) {
    return protohdr_bytes;
  } else {
    return recv_message_ph_.len - protohdr_bytes;
  }
}

void Connection::eventCallback(struct bufferevent* bev, void* arg, short what) {
  Connection* self = reinterpret_cast<Connection*>(arg);

  ld_check(self);
  ld_check(bev == self->bev_);

  SocketEvent e{what, errno};

  if (self->isSSL() && !(e.what & BEV_EVENT_CONNECTED)) {
    // libevent's SSL handlers will call this before calling the
    // bytesSentCallback(), which breaks assumptions in our code. To avoid
    // that, we place the callback on a queue instead of calling it
    // immediately.

    // Not deferring onConnected(), as otherwise onConnectAttemptTimeout()
    // might be triggered after the connection has been established (and the
    // BEV_EVENT_CONNECTED processed), but before onConnected() callback is
    // hit.
    self->enqueueDeferredEvent(e);
  } else {
    self->eventCallbackImpl(e);
  }
}

void Connection::eventCallbackImpl(SocketEvent e) {
  STAT_INCR(deps_->getStats(), sock_misc_socket_events);
  auto start_time = std::chrono::steady_clock::now();
  if (e.what & BEV_EVENT_CONNECTED) {
    onConnected();
    auto total_time = getTimeDiff(start_time);
    STAT_ADD(
        deps_->getStats(), sock_connect_event_proc_time, total_time.count());
  } else if (e.what & BEV_EVENT_ERROR) {
    onError(e.what & (BEV_EVENT_READING | BEV_EVENT_WRITING), e.socket_errno);
    auto total_time = getTimeDiff(start_time);
    STAT_ADD(deps_->getStats(), sock_error_event_proc_time, total_time.count());
  } else if (e.what & BEV_EVENT_EOF) {
    onPeerClosed();
    auto total_time = getTimeDiff(start_time);
    STAT_ADD(deps_->getStats(),
             sock_peer_closed_event_proc_time,
             total_time.count());
  } else {
    // BEV_EVENT_TIMEOUT must not be reported yet
    ld_critical("INTERNAL ERROR: unexpected event bitset in a bufferevent "
                "callback: 0x%hx",
                e.what);
    ld_check(0);
  }
}

void Connection::flushNextInSerializeQueue() {
  ld_check(!serializeq_.empty());

  std::unique_ptr<Envelope> next_envelope(&serializeq_.front());
  serializeq_.pop_front();
  send(std::move(next_envelope));
}

void Connection::flushSerializeQueue() {
  while (!serializeq_.empty()) {
    flushNextInSerializeQueue();
  }
}

void Connection::transitionToConnected() {
  addHandshakeTimeoutEvent();
  connected_ = true;
  peer_shuttingdown_ = false;

  ld_debug(
      "Socket(%p) to node %s has connected", this, conn_description_.c_str());

  ld_check(!serializeq_.empty());
  flushNextInSerializeQueue();
}

void Connection::onConnected() {
  auto g = folly::makeGuard(deps_->setupContextGuard());
  ld_check(!isClosed());
  if (expecting_ssl_handshake_) {
    ld_check(connected_);
    // we receive a BEV_EVENT_CONNECTED for an _incoming_ connection after the
    // handshake is done.
    ld_check(isSSL());
    ld_debug("SSL handshake with %s completed", conn_description_.c_str());
    expecting_ssl_handshake_ = false;
    expectProtocolHeader();
    return;
  }
  ld_check(!connected_);
  ld_check(!peer_name_.isClientAddress());

  connect_timeout_event_.cancelTimeout();
  transitionToConnected();
}

void Connection::onSent(std::unique_ptr<Envelope> e,
                        Status reason,
                        Message::CompletionMethod cm) {
  auto g = folly::makeGuard(deps_->setupContextGuard());
  // Do not call onSent() of pending messages if our Worker is getting
  // destroyed. This is to guarantee that onSent() code and the methods
  // it calls do not try to access a partially destroyed Worker, with some
  // members already destroyed and free'd.
  ld_check(!e->links_.is_linked());

  if (reason == Status::OK) {
    FLOW_GROUP_MSG_STAT_INCR(
        deps_->getStats(), flow_group_, &e->message(), sent_ok);
    FLOW_GROUP_MSG_STAT_ADD(
        deps_->getStats(), flow_group_, &e->message(), sent_bytes, e->cost());
  } else {
    FLOW_GROUP_MSG_STAT_INCR(
        deps_->getStats(), flow_group_, &e->message(), sent_error);
  }

  if (!deps_->shuttingDown()) {
    deps_->noteBytesDrained(e->cost(), getPeerType(), e->message().type_);
    deps_->onSent(e->moveMessage(), peer_name_, reason, e->birthTime(), cm);
    ld_check(!e->haveMessage());
  }
}

void Connection::onError(short direction, int socket_errno) {
  auto g = folly::makeGuard(deps_->setupContextGuard());
  // DeferredEventQueue is cleared as part of socket close which can call
  // onError recursively. Check if this is recursive call and skip the check.
  if (closing_) {
    return;
  }

  if (isClosed()) {
    ld_critical("INTERNAL ERROR: got a libevent error on disconnected socket "
                "with peer %s. errno=%d (%s)",
                conn_description_.c_str(),
                socket_errno,
                strerror(socket_errno));
    ld_check(0);
    return;
  }

  bool ssl_error_reported = false;
  if (isSSL()) {
    unsigned long ssl_err = 0;
    char ssl_err_string[120];
    while ((ssl_err = bufferevent_get_openssl_error(bev_))) {
      ERR_error_string_n(ssl_err, ssl_err_string, sizeof(ssl_err_string));
      RATELIMIT_ERROR(
          std::chrono::seconds(10), 10, "SSL error: %s", ssl_err_string);
      ssl_error_reported = true;
    }
  }

  if (connected_) {
    // OpenSSL/libevent error reporting is weird and maybe broken.
    // (Note: make sure to not confuse "SSL_get_error" and "ERR_get_error".)
    // The way openssl reports errors is that you check SSL_get_error(),
    // and if it reports an error, the details supposedly can be found either
    // through ERR_get_error() (for ssl-specific errors) or through
    // errno (for socket errors).
    // But sometimes neither ERR_get_error() nor errno have anything;
    // in particular, I've seen SSL_do_handshake() return -1, then
    // SSL_get_error() return SSL_ERROR_SYSCALL, then ERR_get_error() return 0,
    // and errno = 0.
    // This may deserve an investigation, but for now let's just say
    // "OpenSSL didn't report any details about the error".
    // (Note that bufferevent_get_openssl_error() just propagates errors
    // reported by ERR_get_error().)
    const bool severe = socket_errno != ECONNRESET &&
        socket_errno != ETIMEDOUT && !ssl_error_reported;
    ld_log(severe ? facebook::logdevice::dbg::Level::ERROR
                  : facebook::logdevice::dbg::Level::WARNING,
           "Got an error on socket connected to %s while %s%s. %s",
           conn_description_.c_str(),
           (direction & BEV_EVENT_WRITING) ? "writing" : "reading",
           expecting_ssl_handshake_ ? " (during SSL handshake)" : "",
           socket_errno != 0 ? ("errno=" + std::to_string(socket_errno) + " (" +
                                strerror(socket_errno) + ")")
                                   .c_str()
                             : ssl_error_reported
                   ? "See SSL errors above."
                   : "OpenSSL didn't report any details about the error.");
  } else {
    ld_check(!peer_name_.isClientAddress());
    RATELIMIT_LEVEL(
        socket_errno == ECONNREFUSED ? dbg::Level::DEBUG : dbg::Level::WARNING,
        std::chrono::seconds(10),
        10,
        "Failed to connect to node %s. errno=%d (%s)",
        conn_description_.c_str(),
        socket_errno,
        strerror(socket_errno));
  }

  close(E::CONNFAILED);
}

void Connection::onPeerClosed() {
  auto g = folly::makeGuard(deps_->setupContextGuard());
  // This method can be called recursively as part of Socket::close when
  // deferred event queue is cleared. Return rightaway if this a recursive call.
  if (closing_) {
    return;
  }
  ld_spew("Peer %s closed.", conn_description_.c_str());
  ld_check(!isClosed());
  if (!isSSL()) {
    // an SSL socket can be in a state where the TCP connection is established,
    // but the SSL handshake hasn't finished, this isn't considered connected.
    ld_check(connected_);
  }

  Status reason = E::PEER_CLOSED;

  if (!peer_name_.isClientAddress()) {
    if (peer_shuttingdown_) {
      reason = E::SHUTDOWN;
    }
  }

  close(reason);
}

void Connection::onConnectTimeout() {
  auto g = folly::makeGuard(deps_->setupContextGuard());
  ld_spew("Connection timeout connecting to %s", conn_description_.c_str());

  close(E::TIMEDOUT);
}

void Connection::onHandshakeTimeout() {
  auto g = folly::makeGuard(deps_->setupContextGuard());
  RATELIMIT_WARNING(std::chrono::seconds(10),
                    10,
                    "Handshake timeout occurred (peer: %s).",
                    conn_description_.c_str());
  onConnectTimeout();
  STAT_INCR(deps_->getStats(), handshake_timeouts);
}

void Connection::onConnectAttemptTimeout() {
  auto g = folly::makeGuard(deps_->setupContextGuard());
  ld_check(!connected_);

  RATELIMIT_DEBUG(std::chrono::seconds(5),
                  5,
                  "Connection timeout occurred (peer: %s). Attempt %lu.",
                  conn_description_.c_str(),
                  retries_so_far_);
  ld_check(!connected_);
  if (retries_so_far_ >= getSettings().connection_retries) {
    onConnectTimeout();
    STAT_INCR(deps_->getStats(), connection_timeouts);
  } else {
    // Nothing should be written in the output buffer of an unconnected socket.
    ld_check(getTotalOutbufLength() == 0);
    deps_->buffereventFree(bev_); // this also closes the TCP socket
    bev_ = nullptr;
    ssl_context_.reset();
    conn_closed_->store(true);

    // Try connecting again.
    if (doConnectAttempt() != 0) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "Connect attempt #%lu failed (peer:%s), err=%s",
                        retries_so_far_ + 1,
                        conn_description_.c_str(),
                        error_name(err));
      onConnectTimeout();
    } else {
      STAT_INCR(deps_->getStats(), connection_retries);
      ++retries_so_far_;
    }
  }
}

void Connection::setDSCP(uint8_t dscp) {
  int rc = 0;
  rc = deps_->setDSCP(fd_, peer_sockaddr_.family(), dscp);

  // DSCP is used for external traffic shaping. Allow the connection to
  // continue to operate, but warn about the failure.
  if (rc != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "DSCP(0x%x) configuration failed: %s",
                    dscp,
                    strerror(errno));
  }
}

void Connection::setSoMark(uint32_t so_mark) {
  const int rc = deps_->setSoMark(fd_, so_mark);

  if (rc != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "SO_MARK(0x%x) configuration failed: %s",
                    so_mark,
                    strerror(errno));
  }
}

void Connection::close(Status reason) {
  auto g = folly::makeGuard(deps_->setupContextGuard());
  ld_debug("Closing Socket %s, reason %s ",
           conn_description_.c_str(),
           error_name(reason));
  // Checking and setting this here to prevent recursive closes
  if (closing_) {
    return;
  }
  closing_ = true;
  SCOPE_EXIT {
    closing_ = false;
  };

  if (isClosed()) {
    return;
  }

  *conn_closed_ = true;

  RATELIMIT_LEVEL((reason == E::CONNFAILED || reason == E::TIMEDOUT)
                      ? dbg::Level::DEBUG
                      : dbg::Level::INFO,
                  std::chrono::seconds(10),
                  10,
                  "Closing socket %s. Reason: %s",
                  conn_description_.c_str(),
                  error_description(reason));

  if (getBytesPending() > 0) {
    ld_debug("Socket %s had %zu bytes pending when closed.",
             conn_description_.c_str(),
             getBytesPending());

    ld_debug("Sender now has %zu total bytes pending",
             deps_->getBytesPending() - getBytesPending());
  }

  endStreamRewind();

  if (connect_throttle_ && (peer_shuttingdown_ || reason != E::SHUTDOWN)) {
    connect_throttle_->connectFailed();
  }

  if (!deferred_event_queue_.empty()) {
    // Process outstanding deferred events since they may inform us that
    // connection throttling is appropriate against future connections. But
    // if we are shutting down and won't be accepting new connections, don't
    // bother.
    if (!deps_->shuttingDown()) {
      processDeferredEventQueue();
    } else {
      deferred_event_queue_.clear();
    }
  }

  if (legacy_connection_) {
    ld_check(deps_->attachedToLegacyEventBase());
    // This means that bufferevent was created and should be valid here.
    ld_check(bev_);
    size_t buffered_bytes = LD_EV(evbuffer_get_length)(deps_->getOutput(bev_));

    if (buffered_output_) {
      buffered_bytes += LD_EV(evbuffer_get_length)(buffered_output_);
      buffered_output_flush_event_.cancelTimeout();
      LD_EV(evbuffer_free)(buffered_output_);
      buffered_output_ = nullptr;
    }

    if (isSSL()) {
      deps_->buffereventShutDownSSL(bev_);
    }

    if (buffered_bytes != 0 && !deps_->shuttingDown()) {
      deps_->noteBytesDrained(buffered_bytes,
                              getPeerType(),
                              /* message_type */ folly::none);
    }

    deps_->buffereventFree(bev_); // this also closes the TCP socket
    bev_ = nullptr;
  } else {
    size_t buffered_bytes = getBufferedBytesSize();
    // Clear read callback on close.
    proto_handler_->sock()->setReadCB(nullptr);
    if (buffered_bytes != 0 && !deps_->shuttingDown()) {
      getDeps()->noteBytesDrained(buffered_bytes,
                                  getPeerType(),
                                  /* message_type */ folly::none);
    }
    sock_write_cb_.clear();
    sendChain_.reset();
    sched_write_chain_.cancelTimeout();
    // Invoke closeNow to close the socket.
    proto_handler_->sock()->closeNow();
  }

  markDisconnectedOnClose();
  clearConnQueues(reason);
  STAT_DECR(deps_->getStats(), num_connections);
  if (isSSL()) {
    STAT_DECR(deps_->getStats(), num_ssl_connections);
  }
}
void Connection::markDisconnectedOnClose() {
  // socket was just closed; make sure it's properly accounted for
  conn_incoming_token_.release();
  conn_external_token_.release();

  our_name_at_peer_ = ClientID::INVALID;
  connected_ = false;
  handshaken_ = false;
  ssl_context_.reset();
  peer_config_version_ = config_version_t(0);

  read_more_.cancelTimeout();
  connect_timeout_event_.cancelTimeout();
  handshake_timeout_event_.cancelTimeout();
  deferred_event_queue_event_.cancelTimeout();
  end_stream_rewind_event_.cancelTimeout();
}

void Connection::clearConnQueues(Status close_reason) {
  // Move everything here so that this Socket object has a clean state
  // before we call any callback.
  PendingQueue moved_pendingq = std::move(pendingq_);
  std::vector<EnvelopeQueue> moved_queues;
  moved_queues.emplace_back(std::move(serializeq_));
  moved_queues.emplace_back(std::move(sendq_));
  folly::IntrusiveList<SocketCallback, &SocketCallback::listHook_>
      on_close_moved = std::move(impl_->on_close_);
  folly::IntrusiveList<BWAvailableCallback, &BWAvailableCallback::links_>
      pending_bw_cbs_moved = std::move(impl_->pending_bw_cbs_);

  ld_check(pendingq_.empty());
  ld_check(serializeq_.empty());
  ld_check(sendq_.empty());
  ld_check(impl_->on_close_.empty());
  ld_check(impl_->pending_bw_cbs_.empty());
  ld_check(deferred_event_queue_.empty());

  for (auto& queue : moved_queues) {
    while (!queue.empty()) {
      std::unique_ptr<Envelope> e(&queue.front());
      queue.pop_front();
      onSent(std::move(e), close_reason);
    }
  }

  // Clients expect all outstanding messages to be completed prior to
  // delivering "on close" callbacks.
  if (!deps_->shuttingDown()) {
    moved_pendingq.trim(
        Priority::MAX, moved_pendingq.cost(), [&](Envelope& e_ref) {
          std::unique_ptr<Envelope> e(&e_ref);
          onSent(std::move(e), close_reason);
        });
    ld_check(moved_pendingq.empty());
    // If there are any injected errors they need to be completed before on
    // close callbacks.
    deps_->processDeferredMessageCompletions();
  }

  // Mark next and drain pos as the same to make sure getBufferedBytesSize()
  // returns zero going forward.
  drain_pos_ = next_pos_;
  ld_check(getBufferedBytesSize() == 0);
  while (!pending_bw_cbs_moved.empty()) {
    auto& cb = pending_bw_cbs_moved.front();
    cb.deactivate();
    cb.cancelled(close_reason);
  }

  while (!on_close_moved.empty()) {
    auto& cb = on_close_moved.front();
    on_close_moved.pop_front();

    // on_close_ is an intrusive list, pop_front() removes cb from list but
    // does not call any destructors. cb is now not on any callback lists.
    cb(close_reason, peer_name_);
  }
}

bool Connection::isClosed() const {
  auto g = folly::makeGuard(deps_->setupContextGuard());
  if (conn_closed_ != nullptr &&
      !conn_closed_->load(std::memory_order_relaxed)) {
    return false;
  }
  ld_check(!connected_);
  ld_check(sendq_.empty());
  ld_check(serializeq_.empty());
  // When the socket is getting closed the getBufferedBytesSize will be
  // incorrect as we have not cleared all the members , hence skip the
  // getBytesPending check.
  ld_check(closing_ || getBytesPending() == 0);
  return true;
}

bool Connection::good() const {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  auto is_good = !isClosed();
  if (!legacy_connection_) {
    return is_good && proto_handler_->good();
  }

  return is_good;
}

bool Connection::sizeLimitsExceeded() const {
  return getBytesPending() > outbuf_overflow_;
}

bool Connection::isChecksummingEnabled(MessageType msgtype) {
  if (!getSettings().checksumming_enabled) {
    return false;
  }

  auto& msg_checksum_set = getSettings().checksumming_blacklisted_messages;
  return msg_checksum_set.find((char)msgtype) == msg_checksum_set.end();
}

std::unique_ptr<folly::IOBuf> Connection::serializeMessage(const Message& msg) {
  const bool compute_checksum =
      ProtocolHeader::needChecksumInHeader(msg.type_, proto_) &&
      isChecksummingEnabled(msg.type_);

  const size_t protohdr_bytes = ProtocolHeader::bytesNeeded(msg.type_, proto_);
  auto io_buf = folly::IOBuf::create(IOBUF_ALLOCATION_UNIT);
  ld_check(protohdr_bytes <= IOBUF_ALLOCATION_UNIT);
  io_buf->advance(protohdr_bytes);

  ProtocolWriter writer(msg.type_, io_buf.get(), proto_);

  msg.serialize(writer);
  ssize_t bodylen = writer.result();
  if (bodylen <= 0) { // unlikely
    RATELIMIT_CRITICAL(std::chrono::seconds(1),
                       2,
                       "INTERNAL ERROR: Failed to serialize a message of "
                       "type %s into evbuffer",
                       messageTypeNames()[msg.type_].c_str());
    ld_check(0);
    err = E::INTERNAL;
    close(err);
    return nullptr;
  }

  ProtocolHeader protohdr;
  protohdr.cksum = compute_checksum ? writer.computeChecksum() : 0;
  protohdr.cksum += shouldTamperChecksum(); // For Tests only
  protohdr.type = msg.type_;
  io_buf->prepend(protohdr_bytes);
  protohdr.len = io_buf->computeChainDataLength();

  memcpy(static_cast<void*>(io_buf->writableData()), &protohdr, protohdr_bytes);
  return io_buf;
}

Connection::SendStatus
Connection::sendBuffer(std::unique_ptr<folly::IOBuf>&& io_buf) {
  if (legacy_connection_) {
    struct evbuffer* outbuf =
        buffered_output_ ? buffered_output_ : deps_->getOutput(bev_);
    ld_check(outbuf);
    for (auto& buf : *io_buf) {
      int rv = LD_EV(evbuffer_add)(outbuf, buf.data(), buf.size());
      if (rv) {
        size_t outbuf_size = LD_EV(evbuffer_get_length)(outbuf);
        RATELIMIT_CRITICAL(std::chrono::seconds(1),
                           2,
                           "INTERNAL ERROR: Failed to move iobuffers to "
                           "outbuf, from io_buf"
                           "(io_buf_size:%zu, outbuf:%zu)",
                           io_buf->computeChainDataLength(),
                           outbuf_size);
        err = E::INTERNAL;
        close(err);
        return Connection::SendStatus::ERROR;
      }
    }
  } else if (proto_handler_->good()) { // Sending data over new connection.
    if (sendChain_) {
      ld_check(sched_write_chain_.isScheduled());
      sendChain_->prependChain(std::move(io_buf));
    } else {
      sendChain_ = std::move(io_buf);
      ld_check(!sched_write_chain_.isScheduled());
      sched_write_chain_.attachCallback([this]() { scheduleWriteChain(); });
      sched_write_chain_.scheduleTimeout(0);
      sched_start_time_ = SteadyTimestamp::now();
    }
  }
  return Connection::SendStatus::SCHEDULED;
}

void Connection::scheduleWriteChain() {
  auto g = folly::makeGuard(deps_->setupContextGuard());
  ld_check(!legacy_connection_);
  if (!proto_handler_->good()) {
    return;
  }
  ld_check(sendChain_);
  auto now = SteadyTimestamp::now();
  STAT_ADD(deps_->getStats(),
           sock_write_sched_delay,
           to_msec(now - sched_start_time_).count());

  // Get bytes that are added to sendq but not yet added in the asyncSocket.
  auto bytes_in_sendq = getBufferedBytesSize() - sock_write_cb_.bytes_buffered;
  sock_write_cb_.write_chains.emplace_back(
      SocketWriteCallback::WriteUnit{bytes_in_sendq, now});
  // These bytes are now buffered in socket and will be removed from sendq.
  sock_write_cb_.bytes_buffered += bytes_in_sendq;
  proto_handler_->sock()->writeChain(&sock_write_cb_, std::move(sendChain_));
  // All the bytes will be now removed from sendq now that we have written into
  // the asyncsocket.
  onBytesAdmittedToSend(bytes_in_sendq);
}

int Connection::serializeMessage(std::unique_ptr<Envelope>&& envelope) {
  // We should only write to the output buffer once connected.
  ld_check(connected_);

  const auto& msg = envelope->message();

  std::unique_ptr<folly::IOBuf> serialized_buf = serializeMessage(msg);

  if (serialized_buf == nullptr) {
    return -1;
  }

  const auto msglen = serialized_buf->computeChainDataLength();
  Connection::SendStatus status = sendBuffer(std::move(serialized_buf));
  if (status == Connection::SendStatus::ERROR) {
    RATELIMIT_CRITICAL(std::chrono::seconds(1),
                       2,
                       "INTERNAL ERROR: Failed to send a message of "
                       "type %s",
                       messageTypeNames()[msg.type_].c_str());
    return -1;
  }

  MESSAGE_TYPE_STAT_INCR(deps_->getStats(), msg.type_, message_sent);
  TRAFFIC_CLASS_STAT_INCR(deps_->getStats(), msg.tc_, messages_sent);
  TRAFFIC_CLASS_STAT_ADD(deps_->getStats(), msg.tc_, bytes_sent, msglen);

  ld_check(!isHandshakeMessage(msg.type_) || next_pos_ == 0);
  ld_check(next_pos_ >= drain_pos_);

  deps_->noteBytesQueued(msglen, getPeerType(), /* message_type */ folly::none);
  if (status == Connection::SendStatus::SCHEDULED) {
    next_pos_ += msglen;
    envelope->setDrainPos(next_pos_);

    envelope->enqTime(std::chrono::steady_clock::now());
    sendq_.push_back(*envelope.release());
    ld_check(!envelope);
    auto& s = health_stats_;
    // Check if bytes in socket is above idle_threshold. Accumulate active bytes
    // sent and change state to active if necessary.
    if (getBufferedBytesSize() > getSettings().socket_idle_threshold &&
        s.active_start_time_ == SteadyTimestamp::min()) {
      s.active_start_time_ = deps_->getCurrentTimestamp();
    }
  }
  if (status == Connection::SendStatus::SENT) {
    STAT_INCR(deps_->getStats(), sock_num_messages_sent);
    STAT_ADD(deps_->getStats(), sock_total_bytes_in_messages_written, msglen);
    ld_check(status == Connection::SendStatus::SENT);
    // Some state machines expect onSent for success scenarios to be called
    // after completion of sendMessage invocation. Hence, we need to post a
    // function to invoke onSent later.
    auto exec = deps_->getExecutor();
    ld_check(exec);
    auto sent_success = [this,
                         is_closed =
                             std::weak_ptr<std::atomic<bool>>(conn_closed_),
                         e = std::move(envelope)]() mutable {
      auto ref = is_closed.lock();
      if (ref && !*ref) {
        auto g = folly::makeGuard(deps_->setupContextGuard());
        onSent(std::move(e), E::OK);
      }
    };
    if (exec->getNumPriorities() > 1) {
      exec->addWithPriority(std::move(sent_success), folly::Executor::HI_PRI);
    } else {
      exec->add(std::move(sent_success));
    }
  }
  return 0;
}

bool Connection::injectAsyncMessageError(std::unique_ptr<Envelope>&& e) {
  auto error_chance_percent =
      getSettings().message_error_injection_chance_percent;
  auto error_status = getSettings().message_error_injection_status;
  if (error_chance_percent != 0 &&
      error_status != E::CBREGISTERED && // Must be synchronously delivered
      !isHandshakeMessage(e->message().type_) && !closing_ &&
      !message_error_injection_rewinding_stream_) {
    if (folly::Random::randDouble(0, 100.0) <= error_chance_percent) {
      message_error_injection_rewinding_stream_ = true;
      // Turn off the rewind when the deferred event queue is drained.
      // Ensure this happens even if no other deferred events are added
      // for this socket during the current event loop cycle.
      end_stream_rewind_event_.activate(EV_WRITE, 0);
      ld_error("Rewinding Stream on Socket (%p) - %jd passed, %01.8f%% chance",
               this,
               (intmax_t)message_error_injection_pass_count_,
               error_chance_percent);
      message_error_injection_pass_count_ = 0;
    }
  }

  if (message_error_injection_rewinding_stream_) {
    message_error_injection_rewound_count_++;
    onSent(std::move(e), error_status, Message::CompletionMethod::DEFERRED);
    return true;
  }

  message_error_injection_pass_count_++;
  return false;
}

int Connection::preSendCheck(const Message& msg) {
  if (isClosed()) {
    err = E::NOTCONN;
    return -1;
  }

  if (!handshaken_) {
    if (peer_name_.isClientAddress() && !isACKMessage(msg.type_)) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "attempt to send a message of type %s to client %s "
                      "before handshake was completed",
                      messageTypeNames()[msg.type_].c_str(),
                      conn_description_.c_str());
      err = E::UNREACHABLE;
      return -1;
    }
  } else if (msg.getMinProtocolVersion() > proto_) {
    if (msg.warnAboutOldProtocol()) {
      RATELIMIT_WARNING(
          std::chrono::seconds(1),
          10,
          "Could not serialize message of type %s to Socket %s "
          "because messages expects a protocol version >= %hu but "
          "the protocol used for that socket is %hu",
          messageTypeNames()[msg.type_].c_str(),
          conn_description_.c_str(),
          msg.getMinProtocolVersion(),
          proto_);
    }

    if (isHandshakeMessage(msg.type_)) {
      ld_critical("INTERNAL ERROR: getMinProtocolVersion() is expected to "
                  "return a protocol version <= %hu for a message of type %s,"
                  " but it returns %hu instead.",
                  proto_,
                  messageTypeNames()[msg.type_].c_str(),
                  msg.getMinProtocolVersion());
      close(E::INTERNAL);
      err = E::INTERNAL;
      ld_check(0);
    }

    err = E::PROTONOSUPPORT;
    return -1;
  }

  return 0;
}

void Connection::send(std::unique_ptr<Envelope> envelope) {
  const auto& msg = envelope->message();

  if (preSendCheck(msg)) {
    onSent(std::move(envelope), err);
    return;
  }

  if (msg.cancelled()) {
    onSent(std::move(envelope), E::CANCELLED);
    return;
  }

  // If we are handshaken, serialize the message directly to the output
  // buffer. Otherwise, push the message to the serializeq_ queue, it will be
  // serialized once we are handshaken. An exception is handshake messages,
  // they can be serialized as soon as we are connected.
  if (handshaken_ || (connected_ && isHandshakeMessage(msg.type_))) {
    // compute the message length only when 1) handshaken is completed and
    // negotiaged proto_ is known; or 2) message is a handshaken message
    // therefore its size does not depend on the protocol
    const auto msglen = msg.size(proto_);
    if (msglen > Message::MAX_LEN + sizeof(ProtocolHeader)) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          2,
          "Tried to send a message that's too long (%lu bytes) to %s",
          (size_t)msglen,
          conn_description_.c_str());
      err = E::TOOBIG;
      onSent(std::move(envelope), err);
      return;
    }

    // Offer up the message for error injection first. If the message
    // is accepted for injected error delivery, our responsibility for
    // sending the message ends.
    if (injectAsyncMessageError(std::move(envelope))) {
      return;
    }

    if (serializeMessage(std::move(envelope)) != 0) {
      ld_check(err == E::INTERNAL || err == E::PROTONOSUPPORT);
      onSent(std::move(envelope), err);
      return;
    }
  } else {
    serializeq_.push_back(*envelope.release());
  }
}

Envelope* Connection::registerMessage(std::unique_ptr<Message>&& msg) {
  if (preSendCheck(*msg) != 0) {
    return nullptr;
  }

  // MessageType::HELLO and ::ACK are excluded from these limits because
  // we want to be able to establish connections even if we are out of
  // buffer space for messages. HELLO and ACK are a part of connection
  // establishment.
  if (!isHandshakeMessage(msg->type_) && sizeLimitsExceeded()) {
    RATELIMIT_WARNING(
        std::chrono::seconds(1),
        10,
        "ENOBUFS for Socket %s. Current socket usage: %zu, max: %zu",
        conn_description_.c_str(),
        getBytesPending(),
        outbuf_overflow_);

    RATELIMIT_INFO(std::chrono::seconds(60),
                   1,
                   "Messages queued to %s: %s",
                   peer_name_.toString().c_str(),
                   deps_->dumpQueuedMessages(peer_name_).c_str());
    err = E::NOBUFS;
    return nullptr;
  }

  auto envelope = std::make_unique<Envelope>(*this, std::move(msg));
  ld_check(!msg);

  pendingq_.push(*envelope);
  deps_->noteBytesQueued(
      envelope->cost(), getPeerType(), envelope->message().type_);

  return envelope.release();
}

void Connection::releaseMessage(Envelope& envelope) {
  // This envelope should be in the pendingq_.
  ld_check(envelope.links_.is_linked());

  // If this envelope was registered as a deferred callback on this
  // socket's FlowGroup, the code releasing the envelope should
  // have dequeued it.
  ld_check(!envelope.active());

  // Take ownership of the envelope
  std::unique_ptr<Envelope> pending_envelope(&envelope);
  pendingq_.erase(*pending_envelope);

  FLOW_GROUP_MSG_LATENCY_ADD(deps_->getStats(), flow_group_, envelope);

  send(std::move(pending_envelope));
}

std::unique_ptr<Message> Connection::discardEnvelope(Envelope& envelope) {
  // This envelope should be in the pendingq_.
  ld_check(envelope.links_.is_linked());

  deps_->noteBytesDrained(
      envelope.cost(), getPeerType(), envelope.message().type_);

  // Take ownership of the envelope so it is deleted.
  std::unique_ptr<Envelope> pending_envelope(&envelope);
  pendingq_.erase(*pending_envelope);

  // The caller decides the disposition of the enclosed message.
  return pending_envelope->moveMessage();
}

void Connection::sendHello() {
  ld_check(!isClosed());
  ld_check(!connected_);
  ld_check(next_pos_ == 0);
  ld_check(drain_pos_ == 0);

  // HELLO should be the first message to be sent on this socket.
  ld_check(getBytesPending() == 0);

  auto hello = deps_->createHelloMessage(peer_name_.asNodeID());
  auto envelope = registerMessage(std::move(hello));
  ld_check(envelope);
  releaseMessage(*envelope);
}

void Connection::sendShutdown() {
  ld_check(!isClosed());

  auto shutdown = deps_->createShutdownMessage(deps_->getServerInstanceId());
  auto envelope = registerMessage(std::move(shutdown));
  // envelope could be null if presend check failed (becasue
  // handshake is not complete) or there was no buffer space. In
  // either case, no shutdown will be sent.
  if (envelope) {
    releaseMessage(*envelope);
  }
}

const Settings& Connection::getSettings() {
  return deps_->getSettings();
}

void Connection::bytesSentCallback(struct evbuffer* buffer,
                                   const struct evbuffer_cb_info* info,
                                   void* arg) {
  Connection* self = reinterpret_cast<Connection*>(arg);

  ld_check(self);
  ld_check(!self->isClosed());
  ld_check(buffer == self->deps_->getOutput(self->bev_));
  STAT_INCR(self->deps_->getStats(), sock_write_events);
  if (info->n_deleted > 0) {
    self->onBytesAdmittedToSend(info->n_deleted);
  }
}

void Connection::enqueueDeferredEvent(SocketEvent e) {
  deferred_event_queue_.push_back(e);

  if (!deferred_event_queue_event_.isScheduled()) {
    ld_check(deferred_event_queue_event_.scheduleTimeout(0));
  }
}

void Connection::onBytesAdmittedToSend(size_t nbytes) {
  auto g = folly::makeGuard(deps_->setupContextGuard());
  message_pos_t next_drain_pos = drain_pos_ + nbytes;
  ld_check(next_pos_ >= next_drain_pos);
  size_t num_messages = 0;
  auto start_time = std::chrono::steady_clock::now();

  while (!sendq_.empty() && sendq_.front().getDrainPos() <= next_drain_pos) {
    // All bytes of message at cur have been sent into the underlying socket.
    std::unique_ptr<Envelope> e(&sendq_.front());
    ld_spew("%s: message sent of type %c and size %lu",
            conn_description_.c_str(),
            int(e->message().type_),
            e->message().size());
    sendq_.pop_front();
    STAT_ADD(
        deps_->getStats(), sock_total_time_in_messages_written, e->enqTime());
    // Messages should be serialized only if we are handshaken_. The only
    // exception is the first message which is a handshake message. HELLO and
    // ACK messages are always at pos_ 0 since they are the first messages to
    // be sent on a connected socket.
    if (isHandshakeMessage(e->message().type_)) {
      // HELLO or ACK must be the first thing we ever send through a socket.
      ld_check_eq(drain_pos_, 0);
      ld_check_eq(num_messages, 0);

      if (!peer_name_.isClientAddress()) {
        // It's an outgoing connection, and we're sending HELLO.
        // Socket doesn't allow enqueueing messages until we get an ACK,
        // so the queue should be empty.
        ld_check(!handshaken_);
        ld_check(sendq_.empty());
      }
    } else {
      ld_check(handshaken_);
      if (!our_name_at_peer_.valid()) {
        // It's an incoming connection. The first message we send must be ACK.
        ld_check(drain_pos_ > 0 || num_messages > 0);
      }
    }
    onSent(std::move(e), E::OK);
    ++num_messages_sent_;
    ++num_messages;
  }

  drain_pos_ = next_drain_pos;

  auto total_time = getTimeDiff(start_time);
  STAT_ADD(deps_->getStats(),
           sock_time_spent_to_process_send_done,
           total_time.count());
  STAT_ADD(deps_->getStats(), sock_num_messages_sent, num_messages);
  STAT_ADD(deps_->getStats(), sock_total_bytes_in_messages_written, nbytes);
  if (legacy_connection_) {
    onBytesPassedToTCP(nbytes);
  }
}

void Connection::onBytesPassedToTCP(size_t nbytes) {
  // If we are in active state and bytes were written into the socket, assume
  // that they are already sent to the remote and mark the state as inactive if
  // necessary.
  auto bytes_in_socket = getBufferedBytesSize();
  auto& s = health_stats_;
  s.num_bytes_sent_ += nbytes;
  if (s.active_start_time_ != SteadyTimestamp::min() &&
      bytes_in_socket <= getSettings().socket_idle_threshold) {
    auto diff = deps_->getCurrentTimestamp() - s.active_start_time_;
    s.active_time_ += to_msec(diff);
    s.active_start_time_ = SteadyTimestamp::min();
  }

  deps_->noteBytesDrained(
      nbytes, getPeerType(), /* message_type */ folly::none);

  ld_spew("Socket %s passed %zu bytes to TCP. Sender now has %zu total "
          "bytes pending",
          conn_description_.c_str(),
          nbytes,
          deps_->getBytesPending());
}

void Connection::drainSendQueue() {
  auto g = folly::makeGuard(deps_->setupContextGuard());
  ld_check(!legacy_connection_);
  auto& cb = sock_write_cb_;
  size_t total_bytes_drained = 0;
  for (size_t& i = cb.num_success; i > 0; --i) {
    total_bytes_drained += cb.write_chains.front().length;
    STAT_ADD(deps_->getStats(),
             sock_write_sched_size,
             cb.write_chains.front().length);
    cb.write_chains.pop_front();
  }

  ld_check(cb.bytes_buffered >= total_bytes_drained);
  cb.bytes_buffered -= total_bytes_drained;
  onBytesPassedToTCP(total_bytes_drained);

  // flushOutputAndClose sets close_reason_ and waits for all buffers to drain.
  // Check if all buffers were drained here if that is the case close the
  // connection.
  if (close_reason_ != E::UNKNOWN && cb.write_chains.size() == 0 &&
      !sendChain_) {
    close(close_reason_);
  }
}

void Connection::deferredEventQueueEventCallback(void* instance, short) {
  auto self = reinterpret_cast<Connection*>(instance);
  self->processDeferredEventQueue();
}

void Connection::processDeferredEventQueue() {
  auto& queue = deferred_event_queue_;
  ld_check(!queue.empty());

  while (!queue.empty()) {
    // we have to remove the event from the queue before hitting callbacks, as
    // they might trigger calls into deferredEventQueueEventCallback() as
    // well.
    SocketEvent event = queue.front();
    queue.pop_front();

    // Hitting the callbacks
    eventCallbackImpl(event);
  }

  if (deferred_event_queue_event_.isScheduled()) {
    deferred_event_queue_event_.cancelTimeout();
  }

  ld_check(queue.empty());
  ld_assert(!deferred_event_queue_event_.isScheduled());
}

void Connection::endStreamRewindCallback(void* instance, short) {
  auto self = reinterpret_cast<Connection*>(instance);
  self->endStreamRewind();
}

void Connection::endStreamRewind() {
  if (message_error_injection_rewinding_stream_) {
    ld_error("Ending Error Injection on Socket (%p) - %jd diverted",
             this,
             (intmax_t)message_error_injection_rewound_count_);
    message_error_injection_rewound_count_ = 0;
    message_error_injection_rewinding_stream_ = false;
  }
}

void Connection::expectProtocolHeader() {
  ld_check(!isClosed());
  if (bev_) {
    size_t protohdr_bytes =
        ProtocolHeader::bytesNeeded(recv_message_ph_.type, proto_);

    // Set read watermarks. This tells bev_ to call dataReadCallback()
    // only after sizeof(ProtocolHeader) bytes are available in the input
    // evbuffer (low watermark). bev_ will stop reading from TCP socket after
    // the evbuffer hits tcp_rcvbuf_size_ (high watermark).
    deps_->buffereventSetWatermark(bev_,
                                   EV_READ,
                                   protohdr_bytes,
                                   std::max(protohdr_bytes, tcp_rcvbuf_size_));
  }
  expecting_header_ = true;
}

void Connection::expectMessageBody() {
  ld_check(!isClosed());
  ld_check(expecting_header_);

  if (bev_) {
    size_t protohdr_bytes =
        ProtocolHeader::bytesNeeded(recv_message_ph_.type, proto_);
    ld_check(recv_message_ph_.len > protohdr_bytes);
    ld_check(recv_message_ph_.len <= Message::MAX_LEN + protohdr_bytes);

    deps_->buffereventSetWatermark(
        bev_,
        EV_READ,
        recv_message_ph_.len - protohdr_bytes,
        std::max((size_t)recv_message_ph_.len, tcp_rcvbuf_size_));
  }
  expecting_header_ = false;
}

int Connection::readMessageHeader(struct evbuffer* inbuf) {
  ld_check(expectingProtocolHeader());
  static_assert(sizeof(recv_message_ph_) == sizeof(ProtocolHeader),
                "recv_message_ph_ type is not ProtocolHeader");
  // 1. Read first 2 fields of ProtocolHeader to extract message type
  size_t min_protohdr_bytes =
      sizeof(ProtocolHeader) - sizeof(ProtocolHeader::cksum);
  int nbytes =
      LD_EV(evbuffer_remove)(inbuf, &recv_message_ph_, min_protohdr_bytes);
  if (nbytes != min_protohdr_bytes) { // unlikely
    ld_critical("INTERNAL ERROR: got %d from evbuffer_remove() while "
                "reading a protocol header from peer %s. "
                "Expected %lu bytes.",
                nbytes,
                conn_description_.c_str(),
                min_protohdr_bytes);
    err = E::INTERNAL; // TODO: make sure close() works as an error handler
    return -1;
  }
  if (recv_message_ph_.len <= min_protohdr_bytes) {
    ld_error("PROTOCOL ERROR: got message length %u from peer %s, expected "
             "at least %zu given sizeof(ProtocolHeader)=%zu",
             recv_message_ph_.len,
             conn_description_.c_str(),
             min_protohdr_bytes + 1,
             sizeof(ProtocolHeader));
    err = E::BADMSG;
    return -1;
  }

  size_t protohdr_bytes =
      ProtocolHeader::bytesNeeded(recv_message_ph_.type, proto_);

  if (recv_message_ph_.len > Message::MAX_LEN + protohdr_bytes) {
    err = E::BADMSG;
    ld_error("PROTOCOL ERROR: got invalid message length %u from peer %s "
             "for msg:%s. Expected at most %u. min_protohdr_bytes:%zu",
             recv_message_ph_.len,
             conn_description_.c_str(),
             messageTypeNames()[recv_message_ph_.type].c_str(),
             Message::MAX_LEN,
             min_protohdr_bytes);
    return -1;
  }

  if (!handshaken_ && !isHandshakeMessage(recv_message_ph_.type)) {
    ld_error("PROTOCOL ERROR: got a message of type %s on a brand new "
             "connection to/from %s). Expected %s.",
             messageTypeNames()[recv_message_ph_.type].c_str(),
             conn_description_.c_str(),
             peer_name_.isClientAddress() ? "HELLO" : "ACK");
    err = E::PROTO;
    return -1;
  }

  // 2. Now read checksum field if needed
  if (ProtocolHeader::needChecksumInHeader(recv_message_ph_.type, proto_)) {
    int cksum_nbytes = LD_EV(evbuffer_remove)(
        inbuf, &recv_message_ph_.cksum, sizeof(recv_message_ph_.cksum));

    if (cksum_nbytes != sizeof(recv_message_ph_.cksum)) { // unlikely
      ld_critical("INTERNAL ERROR: got %d from evbuffer_remove() while "
                  "reading checksum in protocol header from peer %s. "
                  "Expected %lu bytes.",
                  cksum_nbytes,
                  conn_description_.c_str(),
                  sizeof(recv_message_ph_.cksum));
      err = E::INTERNAL;
      return -1;
    }
  }
  return 0;
}

bool Connection::verifyChecksum(ProtocolHeader ph, ProtocolReader& reader) {
  size_t protocol_bytes_already_read =
      ProtocolHeader::bytesNeeded(ph.type, proto_);

  auto enabled = isChecksummingEnabled(ph.type) &&
      ProtocolHeader::needChecksumInHeader(ph.type, proto_) && ph.cksum != 0;

  if (!enabled) {
    return true;
  }

  uint64_t cksum_recvd = ph.cksum;
  uint64_t cksum_computed =
      reader.computeChecksum(ph.len - sizeof(ProtocolHeader));

  RATELIMIT_DEBUG(std::chrono::seconds(10),
                  2,
                  "msg:%s, cksum_recvd:%lu, cksum_computed:%lu, msg_len:%u, "
                  "proto_:%hu, protocol_bytes_already_read:%zu",
                  messageTypeNames()[ph.type].c_str(),
                  cksum_recvd,
                  cksum_computed,
                  ph.len,
                  proto_,
                  protocol_bytes_already_read);

  if (cksum_recvd != cksum_computed) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        2,
        "Checksum mismatch (recvd:%lu, computed:%lu) detected with peer %s"
        ", msgtype:%s",
        cksum_recvd,
        cksum_computed,
        conn_description_.c_str(),
        messageTypeNames()[ph.type].c_str());

    err = E::CHECKSUM_MISMATCH;
    STAT_INCR(deps_->getStats(), protocol_checksum_mismatch);
    return false;
  }

  STAT_INCR(deps_->getStats(), protocol_checksum_matched);
  return true;
}

bool Connection::validateReceivedMessage(const Message* msg) const {
  if (isHandshakeMessage(msg->type_)) {
    if (handshaken_) {
      ld_error("PROTOCOL ERROR: got a duplicate %s from %s",
               messageTypeNames()[msg->type_].c_str(),
               conn_description_.c_str());
      err = E::PROTO;
      return false;
    }
  }
  /* verify that gossip sockets don't receive non-gossip messages
   * exceptions: handshake, config synchronization, shutdown
   */
  if (type_ == SocketType::GOSSIP) {
    if (!(msg->type_ == MessageType::SHUTDOWN ||
          allowedOnGossipConnection(msg->type_))) {
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        1,
                        "Received invalid message(%u) on gossip socket",
                        static_cast<unsigned char>(msg->type_));
      err = E::BADMSG;
      return false;
    }
  }

  return true;
}

bool Connection::processHandshakeMessage(const Message* msg) {
  switch (msg->type_) {
    case MessageType::ACK: {
      deps_->processACKMessage(msg, &our_name_at_peer_, &proto_);
      if (connect_throttle_) {
        connect_throttle_->connectSucceeded();
      } else {
        ld_check(connect_throttle_);
      }
    } break;
    case MessageType::HELLO:
      // If this is a newly handshaken client connection, we might want to
      // drop it at this point if we're already over the limit. onReceived()
      // of a handshake message may set peer_node_id_ (if the client
      // connection is in fact from another node in the cluster), which is why
      // the check is not done earlier.
      if (peerIsClient() &&
          !(conn_external_token_ =
                deps_->getConnBudgetExternal().acquireToken())) {
        RATELIMIT_WARNING(std::chrono::seconds(10),
                          1,
                          "Rejecting a client connection from %s because the "
                          "client connection limit has been reached.",
                          conn_description_.c_str());

        // Set to false to prevent close() from releasing even though
        // acquire() failed.
        handshaken_ = false;

        err = E::TOOMANY;
        return false;
      }
      proto_ = deps_->processHelloMessage(msg);
      break;
    default:
      ld_check(false); // unreachable.
  };

  ld_check(proto_ >= Compatibility::MIN_PROTOCOL_SUPPORTED);
  ld_check(proto_ <= Compatibility::MAX_PROTOCOL_SUPPORTED);
  ld_assert(proto_ <= getSettings().max_protocol);
  ld_spew("%s negotiated protocol %d", conn_description_.c_str(), proto_);

  // Now that we know what protocol we are speaking with the other end,
  // we can serialize pending messages. Messages that are not compatible
  // with the protocol will not be sent.
  flushSerializeQueue();

  return true;
}

int Connection::dispatchMessageBody(ProtocolHeader header,
                                    std::unique_ptr<folly::IOBuf> inbuf) {
  auto g = folly::makeGuard(deps_->setupContextGuard());
  recv_message_ph_ = header;
  ProtocolHeader& ph = recv_message_ph_;
  // Tell the Worker that we're processing a message, so it can time it.
  // The time will include message's deserialization, checksumming,
  // onReceived, destructor and Socket's processing overhead.
  RunContext run_context(ph.type);
  deps_->onStartedRunning(run_context);
  SCOPE_EXIT {
    deps_->onStoppedRunning(run_context);
  };

  size_t protocol_bytes_already_read =
      ProtocolHeader::bytesNeeded(ph.type, proto_);
  size_t payload_size = ph.len - protocol_bytes_already_read;

  // Request reservation to add this message into the system.
  auto resource_token = deps_->getResourceToken(payload_size);
  if (!resource_token && !shouldBeInlined(ph.type)) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "INTERNAL ERROR: message of type %s received from peer "
                    "%s is too large: %u bytes to accommodate into the system.",
                    messageTypeNames()[ph.type].c_str(),
                    conn_description_.c_str(),
                    ph.len);
    err = E::NOBUFS;
    if (!legacy_connection_) {
      // No space to push more messages on the worker, disable the read
      // callback. Retry this message and if successful it will add back the
      // ReadCallback.
      ld_check(!retry_receipt_of_message_.isScheduled());
      retry_receipt_of_message_.attachCallback(
          [this, hdr = header, payload = std::move(inbuf)]() mutable {
            if (proto_handler_->dispatchMessageBody(hdr, std::move(payload)) ==
                0) {
              proto_handler_->sock()->setReadCB(read_cb_.get());
            }
          });
      retry_receipt_of_message_.scheduleTimeout(0);
      proto_handler_->sock()->setReadCB(nullptr);
    }
    return -1;
  }

  ProtocolReader reader(ph.type, std::move(inbuf), proto_);

  ++num_messages_received_;
  num_bytes_received_ += ph.len;
  expectProtocolHeader();

  // 1. compute and verify checksum in header.

  if (!verifyChecksum(ph, reader)) {
    ld_check_eq(err, E::CHECKSUM_MISMATCH);
    // converting error type since existing clients don't
    // handle E::CHECKSUM_MISMATCH
    err = E::BADMSG;
    return -1;
  }

  // 2. read and parse message body.

  std::unique_ptr<Message> msg = deps_->deserialize(ph, reader);

  if (!msg) {
    switch (err) {
      case E::TOOBIG:
        ld_error("PROTOCOL ERROR: message of type %s received from peer "
                 "%s is too large: %u bytes",
                 messageTypeNames()[ph.type].c_str(),
                 conn_description_.c_str(),
                 ph.len);
        err = E::BADMSG;
        return -1;

      case E::BADMSG:
        ld_error("PROTOCOL ERROR: message of type %s received from peer "
                 "%s has invalid format. proto_:%hu",
                 messageTypeNames()[ph.type].c_str(),
                 conn_description_.c_str(),
                 proto_);
        err = E::BADMSG;
        return -1;

      case E::INTERNAL:
        ld_critical("INTERNAL ERROR while deserializing a message of type "
                    "%s received from peer %s",
                    messageTypeNames()[ph.type].c_str(),
                    conn_description_.c_str());
        return 0;

      case E::NOTSUPPORTED:
        ld_critical("INTERNAL ERROR: deserializer for message type %d (%s) not "
                    "implemented.",
                    int(ph.type),
                    messageTypeNames()[ph.type].c_str());
        ld_check(false);
        err = E::INTERNAL;
        return -1;

      default:
        ld_critical("INTERNAL ERROR: unexpected error code %d (%s) from "
                    "deserializer for message type %s received from peer %s",
                    static_cast<int>(err),
                    error_name(err),
                    messageTypeNames()[ph.type].c_str(),
                    conn_description_.c_str());
        return 0;
    }

    ld_check(false); // must not get here
    return 0;
  }

  ld_check(msg);

  // 3. Run basic validations.
  if (!validateReceivedMessage(msg.get())) {
    return -1;
  }

  if (isHandshakeMessage(ph.type)) {
    handshaken_ = true;
    first_attempt_ = false;
    handshake_timeout_event_.cancelTimeout();
  }

  MESSAGE_TYPE_STAT_INCR(deps_->getStats(), ph.type, message_received);
  TRAFFIC_CLASS_STAT_INCR(deps_->getStats(), msg->tc_, messages_received);
  TRAFFIC_CLASS_STAT_ADD(deps_->getStats(), msg->tc_, bytes_received, ph.len);

  ld_spew("Received message %s of size %u bytes from %s",
          messageTypeNames()[ph.type].c_str(),
          recv_message_ph_.len,
          conn_description_.c_str());

  // 4. Dispatch message to state machines for processing.

  Message::Disposition disp = deps_->onReceived(
      msg.get(), peer_name_, principal_, std::move(resource_token));

  // 5. Dispose off message according to state machine's request.
  switch (disp) {
    case Message::Disposition::NORMAL:
      // Extra processing for handshake message.
      if (isHandshakeMessage(ph.type) && !processHandshakeMessage(msg.get())) {
        return -1;
      }
      break;
    case Message::Disposition::KEEP:
      // msg may have been deleted or someone might have kept the reference
      // , hence release the reference. It is assumed that the receipient will
      // own this going forward.
      ld_check(!isHandshakeMessage(ph.type));
      msg.release();
      break;
    case Message::Disposition::ERROR:
      // This should be in sync with comment in Message::Disposition enum.
      ld_check_in(err,
                  ({E::ACCESS,
                    E::PROTONOSUPPORT,
                    E::PROTO,
                    E::BADMSG,
                    E::DESTINATION_MISMATCH,
                    E::INVALID_CLUSTER,
                    E::INTERNAL}));
      return -1;
  }

  return 0;
}

int Connection::pushOnCloseCallback(SocketCallback& cb) {
  if (cb.active()) {
    RATELIMIT_CRITICAL(
        std::chrono::seconds(1),
        10,
        "INTERNAL ERROR: attempt to push an active SocketCallback "
        "onto the on_close_ callback list of Socket %s",
        conn_description_.c_str());
    ld_check(false);
    err = E::INVALID_PARAM;
    return -1;
  }

  impl_->on_close_.push_back(cb);
  return 0;
}

int Connection::pushOnBWAvailableCallback(BWAvailableCallback& cb) {
  if (cb.links_.is_linked()) {
    RATELIMIT_CRITICAL(std::chrono::seconds(1),
                       10,
                       "INTERNAL ERROR: attempt to push an active "
                       "BWAvailableCallback onto the pending_bw_cbs_ "
                       "callback list of Socket %s",
                       conn_description_.c_str());
    ld_check(false);
    err = E::INVALID_PARAM;
    return -1;
  }
  impl_->pending_bw_cbs_.push_back(cb);
  return 0;
}

size_t Connection::getTcpSendBufSize() const {
  if (isClosed()) {
    return 0;
  }

  const std::chrono::seconds SNDBUF_CACHE_TTL(1);
  auto now = std::chrono::steady_clock::now();
  if (now - tcp_sndbuf_cache_.update_time >= SNDBUF_CACHE_TTL) {
    tcp_sndbuf_cache_.update_time = now;
    socklen_t optlen = sizeof(int);
    ld_check(fd_ != -1);
    int prev_tcp_sndbuf_size_cache = tcp_sndbuf_cache_.size;
    int rv = getsockopt(
        fd_, SOL_SOCKET, SO_SNDBUF, &tcp_sndbuf_cache_.size, &optlen);
    if (rv == 0) {
      if (tcp_sndbuf_cache_.size > 0) {
        tcp_sndbuf_cache_.size /= 2;
      } else {
        ld_error("getsockopt() returned non-positive number %d: %s",
                 fd_,
                 strerror(errno));
        tcp_sndbuf_cache_.size = prev_tcp_sndbuf_size_cache;
      }
    } else {
      ld_error("Failed to get sndbuf size for TCP socket %d: %s",
               fd_,
               strerror(errno));
      tcp_sndbuf_cache_.size = prev_tcp_sndbuf_size_cache;
    }
  }

  return tcp_sndbuf_cache_.size;
}

size_t Connection::getTcpRecvBufSize() const {
  if (isClosed()) {
    return 0;
  }
  socklen_t optlen = sizeof(int);
  size_t out = 0;
  int rv = getsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &out, &optlen);

  if (rv == 0) {
    out >>= 1; // Response is double of what it really is.
  } else {
    ld_error("Failed to get rcvbuf size for TCP socket %d: %s",
             fd_,
             strerror(errno));
  }
  return out;
}

ssize_t Connection::getTcpRecvBufOccupancy() const {
  if (isClosed()) {
    return -1;
  }
  int ret;
  int error = ioctl(fd_, FIONREAD, &ret);
  if (error != 0) {
    ld_error("Failed to get rcvbuf occupancy for TCP socket %d: %s",
             fd_,
             strerror(error));
    return -1;
  } else {
    return ret;
  }
}

ssize_t Connection::getTcpSendBufOccupancy() const {
  if (isClosed()) {
    return -1;
  }
  int ret;
  int error = ioctl(fd_, TIOCOUTQ, &ret);
  if (error != 0) {
    ld_error("Failed to get sndbuf occupancy for TCP socket %d: %s",
             fd_,
             strerror(error));
    return -1;
  } else {
    return ret;
  }
}

uint64_t Connection::getNumBytesReceived() const {
  return num_bytes_received_;
}

void Connection::addHandshakeTimeoutEvent() {
  std::chrono::milliseconds timeout = getSettings().handshake_timeout;
  if (timeout.count() > 0) {
    handshake_timeout_event_.scheduleTimeout(timeout);
  }
}

void Connection::addConnectAttemptTimeoutEvent() {
  std::chrono::milliseconds timeout = getSettings().connect_timeout;
  if (timeout.count() > 0) {
    timeout *=
        pow(getSettings().connect_timeout_retry_multiplier, retries_so_far_);
    connect_timeout_event_.scheduleTimeout(timeout);
  }
}

size_t Connection::getBytesPending() const {
  size_t queued_bytes = pendingq_.cost() + serializeq_.cost() + sendq_.cost();

  size_t buffered_bytes = 0;
  if (bev_) {
    buffered_bytes += LD_EV(evbuffer_get_length)(deps_->getOutput(bev_));
  }
  if (buffered_output_) {
    buffered_bytes += LD_EV(evbuffer_get_length)(buffered_output_);
  }

  if (!legacy_connection_) {
    buffered_bytes += getBufferedBytesSize();
  }

  return queued_bytes + buffered_bytes;
}

size_t Connection::getBufferedBytesSize() const {
  // This covers the bytes in sendq or in sendChain_ for asyncSocket based
  // implementation.
  size_t buffered_bytes = next_pos_ - drain_pos_;
  // This covers the bytes buffered in asyncsocket.
  if (!legacy_connection_) {
    buffered_bytes += sock_write_cb_.bytes_buffered;
  }
  return buffered_bytes;
}

void Connection::handshakeTimeoutCallback(void* arg, short) {
  reinterpret_cast<Connection*>(arg)->onHandshakeTimeout();
}

void Connection::connectAttemptTimeoutCallback(void* arg, short) {
  reinterpret_cast<Connection*>(arg)->onConnectAttemptTimeout();
}

int Connection::checkConnection(ClientID* our_name_at_peer) {
  if (!our_name_at_peer_.valid()) {
    // socket is either not connected or we're still waiting for a handshake
    // to complete
    ld_check(connect_throttle_);
    if (connect_throttle_ && !connect_throttle_->mayConnect()) {
      ld_check(!connected_);
      ld_check(isClosed());
      err = E::DISABLED;
    } else if (peer_name_.isClientAddress()) {
      err = E::INVALID_PARAM;
    } else if (!isClosed()) {
      err = E::ALREADY;
    } else {
      ld_check(!handshaken_);
      // Sender always initiates a connection attempt whenever a Socket is
      // created. Therefore, we're either still waiting on a connection to be
      // established or are expecting an ACK to complete the handshake. Set
      // err to NOTCONN only if we previously had a working connection to the
      // node.
      err = first_attempt_ ? E::NEVER_CONNECTED : E::NOTCONN;
    }

    return -1;
  }

  if (our_name_at_peer) {
    *our_name_at_peer = our_name_at_peer_;
  }

  return 0;
}

void Connection::dumpQueuedMessages(std::map<MessageType, int>* out) const {
  for (const Envelope& e : sendq_) {
    ++(*out)[e.message().type_];
  }
}

void Connection::getDebugInfo(InfoSocketsTable& table) const {
  std::string state;
  // Connection state of the socket.
  if (isClosed()) {
    state = "I";
  } else if (!connected_) {
    state = "C";
  } else if (!handshaken_) {
    state = "H";
  } else {
    state = "A";
  }

  const size_t available =
      bev_ ? LD_EV(evbuffer_get_length)(deps_->getInput(bev_)) : 0;

  auto total_busy_time = health_stats_.busy_time_.count();
  auto total_rwnd_limited_time = health_stats_.rwnd_limited_time_.count();
  auto total_sndbuf_limited_time = health_stats_.sndbuf_limited_time_.count();
  table.next()
      .set<0>(state)
      .set<1>(deps_->describeConnection(peer_name_))
      .set<2>(getBytesPending() / 1024.0)
      .set<3>(available / 1024.0)
      .set<4>(num_bytes_received_ / 1048576.0)
      .set<5>(drain_pos_ / 1048576.0)
      .set<6>(num_messages_received_)
      .set<7>(num_messages_sent_)
      .set<8>(cached_socket_throughput_)
      .set<9>(total_busy_time == 0
                  ? 0
                  : 100.0 * total_rwnd_limited_time / total_busy_time)
      .set<10>(total_busy_time == 0
                   ? 0
                   : 100.0 * total_sndbuf_limited_time / total_busy_time)
      .set<11>(proto_)
      .set<12>(this->getTcpSendBufSize())
      .set<13>(getPeerConfigVersion().val())
      .set<14>(isSSL())
      .set<15>(fd_);
}

bool Connection::peerIsClient() const {
  return peer_type_ == PeerType::CLIENT;
}

folly::ssl::X509UniquePtr Connection::getPeerCert() const {
  ld_check(isSSL());

  if (legacy_connection_) {
    // This function should only be called when the socket is SSL enabled.
    // This means this should always return a valid ssl context.
    SSL* ctx = bufferevent_openssl_get_ssl(bev_);
    ld_check(ctx);
    return folly::ssl::X509UniquePtr(SSL_get_peer_certificate(ctx));
  }
  auto sock_peer_cert = proto_handler_->sock()->getPeerCertificate();
  if (sock_peer_cert) {
    return sock_peer_cert->getX509();
  }
  return nullptr;
}

SocketDrainStatusType
Connection::getSlowSocketReason(unsigned* net_ltd_pct,
                                unsigned* rwnd_ltd_pct,
                                unsigned* sndbuf_ltd_pct) {
  TCPInfo tcp_info;
  int rv = deps_->getTCPInfo(&tcp_info, fd_);
  if (rv != 0) {
    return SocketDrainStatusType::NET_SLOW;
  }
  auto& s = health_stats_;
  auto cur_busy = to_msec(tcp_info.busy_time > s.busy_time_
                              ? tcp_info.busy_time - s.busy_time_
                              : std::chrono::milliseconds(0));
  auto cur_rwnd =
      to_msec(tcp_info.rwnd_limited_time > s.rwnd_limited_time_
                  ? tcp_info.rwnd_limited_time - s.rwnd_limited_time_
                  : std::chrono::milliseconds(0));
  auto cur_sndbuf =
      to_msec(tcp_info.sndbuf_limited_time > s.sndbuf_limited_time_
                  ? tcp_info.sndbuf_limited_time - s.sndbuf_limited_time_
                  : std::chrono::milliseconds(0));
  s.busy_time_ = to_msec(tcp_info.busy_time);
  s.rwnd_limited_time_ = to_msec(tcp_info.rwnd_limited_time);
  s.sndbuf_limited_time_ = to_msec(tcp_info.sndbuf_limited_time);
  if (cur_busy.count() > 0) {
    *rwnd_ltd_pct = 100.0 * cur_rwnd.count() / cur_busy.count();
    *sndbuf_ltd_pct = 100.0 * cur_sndbuf.count() / cur_busy.count();
    *net_ltd_pct = 100.0 - *rwnd_ltd_pct - *sndbuf_ltd_pct;
    // If network was congested most of the time which prevented from
    // attaining higher throughput mark the socket as slow.
    if (*net_ltd_pct > 50) {
      return SocketDrainStatusType::NET_SLOW;
    }
    if (*rwnd_ltd_pct > 50) {
      return SocketDrainStatusType::RECV_SLOW;
    }
  }

  return SocketDrainStatusType::IDLE;
}

// The socket is either stalled completely or just slow.
// If the socket is stalled completely irrespective of whether it is active
// socket or not we just go ahead and close it in Sender.
// If the socket is not stalled completely.
// 1. Check is made to verify if the socket is an active socket. A socket is
//    active if it has bytes pending for delivery above the
//    socket_idle_threshold for some percentage of socket_health_check period.
// 2. If the socket is inactive socket, it is not closed.
// 3. If the socket is active, check if the socket average throughput when
//    active was way low than expected min_bytes_to_drain_per_second. If this
//    is the case, get the TCPInfo to confirm if the socket has low
//    throughput because of network.
// 4. If network is congested, then we can close the socket if rate limiter
//    allows to do so. In all other cases, socket is not closed.
SocketDrainStatusType Connection::checkSocketHealth() {
  // Close the active window if open.
  auto& s = health_stats_;
  if (s.active_start_time_ != SteadyTimestamp::min()) {
    s.active_time_ +=
        to_msec(deps_->getCurrentTimestamp() - s.active_start_time_);
  }

  SCOPE_EXIT {
    // Reset counters.
    s.active_time_ = std::chrono::milliseconds(0);
    s.num_bytes_sent_ = 0;
    s.active_start_time_ = SteadyTimestamp::min();
    if (getBufferedBytesSize() > getSettings().socket_idle_threshold) {
      s.active_start_time_ = deps_->getCurrentTimestamp();
    }
  };

  std::chrono::milliseconds health_check_period =
      getSettings().socket_health_check_period;
  if (!handshaken_ || health_check_period.count() == 0) {
    return SocketDrainStatusType::UNKNOWN;
  }
  auto age_in_ms = sendq_.size() > 0 ? sendq_.front().age() / 1000 : 0;
  auto is_active = health_check_period.count() *
          getSettings().min_socket_idle_threshold_percent / 100.0 <
      s.active_time_.count();
  double rateKBps = s.num_bytes_sent_ * 1.0 / health_check_period.count();
  cached_socket_throughput_ = rateKBps;
  double min_rateKBps = getSettings().min_bytes_to_drain_per_second / 1e3;
  auto decision = SocketDrainStatusType::UNKNOWN;
  unsigned net_ltd_pct = 0, rwnd_ltd_pct = 0, sndbuf_ltd_pct = 0;
  if (std::chrono::milliseconds(age_in_ms) >
      deps_->getSettings().max_time_to_allow_socket_drain) {
    decision = SocketDrainStatusType::STALLED;
  } else if (!is_active) {
    decision = SocketDrainStatusType::IDLE;
  } else if (rateKBps < min_rateKBps) {
    decision =
        getSlowSocketReason(&net_ltd_pct, &rwnd_ltd_pct, &sndbuf_ltd_pct);
  } else {
    decision = SocketDrainStatusType::ACTIVE;
  }

  if (decision == SocketDrainStatusType::STALLED ||
      (is_active && decision != SocketDrainStatusType::ACTIVE)) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   5,
                   "[%s]: Oldest msg %lums old, throughput %.3fKBps, active "
                   "time %.3fs, decision %s, net %u%%, rwnd %u%%, sndbuf %u%%",
                   peer_name_.toString().c_str(),
                   age_in_ms,
                   rateKBps,
                   s.active_time_.count() / 1e3,
                   socketDrainStatusToString(decision),
                   net_ltd_pct,
                   rwnd_ltd_pct,
                   sndbuf_ltd_pct);
  } else {
    ld_debug(
        "[%s] : Oldest msg age %lums, throughput %.3fKBps, active time %3.fs, "
        "decision %s",
        peer_name_.toString().c_str(),
        age_in_ms,
        rateKBps,
        s.active_time_.count() / 1e3,
        socketDrainStatusToString(decision));
  }
  // Socket is having a normal throughput increment the busy_time for the
  // socket. This is just an estimate, actual busy time might be lesser than
  // this, this avoids a getsockopt call to fetch the busy time.
  if (decision == SocketDrainStatusType::ACTIVE ||
      decision == SocketDrainStatusType::UNKNOWN) {
    s.busy_time_ += s.active_time_;
  }
  return decision;
}
}} // namespace facebook::logdevice
