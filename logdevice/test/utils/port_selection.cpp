/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/test/utils/port_selection.h"

#include <cerrno>
#include <random>

#include <folly/Memory.h>
#include <netinet/in.h>

#include "event2/util.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {
namespace detail {

folly::Optional<PortOwner> claim_port(int port) {
  int rv;
  const Sockaddr addr("::", std::to_string(port));
  int sock = socket(AF_INET6, SOCK_STREAM, 0);

  ld_check(sock != -1);

  // Subprocesses must not inherit this fd
  rv = LD_EV(evutil_make_socket_closeonexec)(sock);
  ld_check(rv == 0);
  // Subprocesses need to be able to bind to this port immediately after we
  // close it
  rv = LD_EV(evutil_make_listen_socket_reuseable)(sock);
  ld_check(rv == 0);

  struct sockaddr_storage ss;
  int len = addr.toStructSockaddr(&ss);
  ld_check(len != -1);
  rv = bind(sock, reinterpret_cast<struct sockaddr*>(&ss), len);
  if (rv != 0) {
    ld_check(errno == EADDRINUSE);
    close(sock);
    return folly::none;
  }

  rv = listen(sock, 0);
  if (rv != 0) {
    ld_check(errno == EADDRINUSE);
    close(sock);
    return folly::none;
  }

  return PortOwner(port, sock);
}

int find_free_port_set(size_t count, std::vector<PortOwner>& ports_out) {
  std::random_device rnd;
  const int port_from = 38000, port_upto = 49000;
  const int port_range_size = port_upto - port_from + 1;
  const int offset =
      std::uniform_int_distribution<int>(0, port_range_size - 1)(rnd);

  std::vector<PortOwner> out;
  for (int i = 0; out.size() < count && i < port_range_size; ++i) {
    int port = port_from + (offset + i) % port_range_size;
    auto owner = claim_port(port);
    if (owner.has_value()) {
      out.push_back(std::move(owner.value()));
    }
  }

  if (out.size() != count) {
    return -1;
  }

  ports_out = std::move(out);
  return 0;
}

}}}} // namespace facebook::logdevice::IntegrationTestUtils::detail
