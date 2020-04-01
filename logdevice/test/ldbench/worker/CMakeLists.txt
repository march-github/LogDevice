# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

auto_sources(hfiles "*.h" RECURSE "${LDBENCH_DIR}/worker")
auto_sources(files "*.cpp" RECURSE "${LDBENCH_DIR}/worker")

REMOVE_MATCHES_FROM_LISTS(files hfiles
  MATCHES
  "Kafka"
  "Test"
)

include_directories(${PYTHON_INCLUDE_DIRS})

add_executable(ldbench ${hfiles} ${files})

target_link_libraries(ldbench
  ldclient
  ${LOGDEVICE_EXTERNAL_DEPS})
