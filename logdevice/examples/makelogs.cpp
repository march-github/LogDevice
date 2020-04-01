/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cctype>
#include <iostream>
#include <string>

#include "logdevice/include/Client.h"
#include "logdevice/include/debug.h"

using namespace facebook::logdevice;

/**
 * @file A basic tool for creating log groups.
 */

static const char* USAGE =
    R"DOC(Usage: ldmakelogs CONFIG PATH LO..HI ATTRIBUTES...

Create a log group at the log tree path PATH in the LogDevice cluster
identified by the config url CONFIG. LO..HI gives the range of integer
log ids in the log group (inclusive). ATTRIBUTES is a list of zero or more
of the following name=value pairs:

replication-factor=N   (default 1)
max-writes-in-flight=N (default 1000)
nodeset-size=N         (default 3)
scd-enabled=yes|no     (default yes)

The rest of log attributes are set to default values.

If a log group or directory already exists at PATH, the tree is left unchanged
and an error is reported. If PATH specifies intermediate directories that
do not exist, those will be created.

)DOC";


static bool validate_config_url(const std::string& config) {
  const std::vector<std::string> supported_schemes = {
    "file:",
    "zk:",
    "zookeeper:"
  };

  for (int i=0; i<supported_schemes.size(); i++) {
    if (config.find(supported_schemes[i])==0)
      return true;
  }

  return false;
}


static void describe_log_group(const client::LogGroup& group) {
  std::cout
    << "Name:  " << group.name() << std::endl
    << "Range: " << (unsigned long)group.range().first << ".."
                 << (unsigned long)group.range().second << std::endl
    << "Version: " << group.version() << std::endl
    << "replicationFactor: "
    << group.attrs().replicationFactor().value() << std::endl
    << "maxWritesInFlight: "
    << group.attrs().maxWritesInFlight().value() << std::endl
    << "nodeSetSize: ";
  if (group.attrs().nodeSetSize().hasValue() &&
      group.attrs().nodeSetSize().value().hasValue())
    std::cout << group.attrs().nodeSetSize().value().value();
  else
    std::cout << "Not set.";
  std::cout
    << std::endl
    << "scdEnabled: " << group.attrs().scdEnabled().value() << std::endl;
}


int main(int argc, const char* argv[]) {

  dbg::currentLevel = dbg::Level::ERROR;

  if (argc <= 1 ||
      strcmp(argv[1], "--help")==0 ||
      strcmp(argv[1], "-h")==0 ||
      strcmp(argv[1], "help")==0) {
    std::cout << USAGE;
    return 0;
  }

  if (argc < 4) {
    std::cout << "Too few arguments.\n\n"
	      << USAGE;
    return 0;
  }

  std::string config(argv[1]);
  if (!validate_config_url(config)) {
    std::cerr
      << "Invalid config URL " << config << std::endl
      << "Expected <scheme>:<locator> where <scheme> is one of " << std::endl
      << "file: zk: zookeeper:" << std::endl;
    return 1;
  }

  std::string path(argv[2]);

  long unsigned lo,hi;

  static_assert(sizeof(logid_t)==sizeof(long unsigned),
		"logid_t size mismatch");

  if (sscanf(argv[3], "%lu..%lu", &lo, &hi) != 2 || lo > hi) {
    std::cerr
      << "Invalid log range " << argv[3] << std::endl
      << "Expected LO..HI where LO<=HI." << std::endl;
    return 1;
  }

  logid_range_t range(lo,hi);

  int replication_factor(1);
  int max_writes_in_flight(1000);
  int nodeset_size(3);
  char scd_enabled='y';

  for (int i=4; i<argc; i++) {
    if (!(sscanf(argv[i], "replication-factor=%d", &replication_factor) == 1 ||
	  sscanf(argv[i], "max-writes-in-flight=%d", &max_writes_in_flight)==1||
	  sscanf(argv[i], "nodeset-size=%d", &nodeset_size)==1 ||
	  sscanf(argv[i], "scd-enabled=%c", &scd_enabled)==1)) {
      std::cerr
	<< "Unknown or incorrectly formatted attribute: "
	<< argv[i] << std::endl
	<< "Expected name=value. See help." << std::endl;
      return 1;
    }
  }

  if (!strchr("ynYN", scd_enabled)) {
    std::cerr
      << "Invalid value of scd-enabled. Expected 'yes' or 'no'." << std::endl;
    return 1;
  }

  std::shared_ptr<Client> client = ClientFactory().create(config);
  if (!client) {
    std::cerr << "Failed to create a LogDevice client" << std::endl;
    return 1;
  }

  std::unique_ptr<client::LogGroup> existing = client->getLogGroupSync(path);

  if (existing) {
    std::cout << "A log group already exists at path " << path << std::endl;
    describe_log_group(*existing);
    return 0;
  }

  client::LogAttributes attrs = client::LogAttributes()
    .with_replicationFactor(replication_factor)
    .with_maxWritesInFlight(max_writes_in_flight)
    .with_nodeSetSize(nodeset_size)
    .with_scdEnabled(tolower(scd_enabled) == 'y');

  std::string errmsg;

  std::unique_ptr<client::LogGroup> group =
    client->makeLogGroupSync(path, range, attrs, true, &errmsg);

  if (!group) {
    std::cerr << "Failed to create log group " << path << std::endl
	      << "Error: " << errmsg << std::endl;
    return 1;
  }

  std::cout
      << "Created a log group with the following attributes:" << std::endl;
  describe_log_group(*group);

  std::cout
    << std::endl
    << "Waiting for config version to propagate to this client..." << std::endl;

  if (!client->syncLogsConfigVersion(group->version())) {
    std::cerr << "syncLogsConfigVersion() failed: " << err << std::endl;
    return 1;
  }

  return 0;
}
