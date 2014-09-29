#!/usr/bin/env python

# Copyright (c) 2014 Spotify AB.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import logging
import types
import uuid

# Set up some basic logging.
logging.basicConfig(level = logging.INFO, format = "[%(asctime)s] [%(levelname)s] %(message)s")

try:
    from kazoo.client import KazooClient
    from kazoo.exceptions import NoNodeError, RuntimeInconsistency
    from kazoo.handlers.threading import TimeoutError
except ImportError:
    logging.error("This script uses Kazoo Python libraries to work with Zookeeper")
    logging.error("You can install them by typing 'sudo easy_install kazoo' in your console")

    # Let the original exception propagate, because sometimes it's not working for different
    # reasons, like package conflicts or whatever else (Python packaging is weird), so it is
    # a good idea to let the user see the actual exception message.
    raise

DESCRIPTION = """
    Bootstraps a new Helios cluster.

    Bootstrapping is done via populating Zookeeper with a basic data structures required
    by Helios to properly function. The script cannot be be used on a NONEMPTY ZooKeeper
    cluster.
"""

def main():
    parser = argparse.ArgumentParser(description = DESCRIPTION)

    parser.add_argument("hosts", metavar = "<zookeeper-endpoint>", type = str,
        nargs = "+", help = "Zookeeper node endpoints to connect to")
    parser.add_argument("--timeout", dest = "timeout", action = "store", type = int,
        default = 30, help = "Zookeeper connection timeout")

    option = parser.parse_args()

    logging.debug("Using %s as a Zookeeper connection string" % option.hosts)

    client = KazooClient(hosts = ",".join(option.hosts))

    try:
        client.start(timeout = option.timeout)
    except TimeoutError as e:
        logging.error("Timed out while connecting to Zookeeper")
        return 1

    status = bootstrap(client, str(uuid.uuid4()))

    # If the client is not stopped, it will hang forever maintaining the connection.
    client.stop()

    return status

def bootstrap(client, cluster_id):
    node_list = [
        "/config",
        "/config/id",
        "/config/id/%s" % cluster_id
    ]

    transaction = client.transaction()

    # Version is not important here. If any of these nodes exist, just stop doing anything and
    # report the error to avoid messing things up.
    [transaction.check(node, version = -1) for node in node_list]

    # Operation results are either True if the given node exists or an exception of NoNodeError or
    # RuntimeIncosistency and RolledBackError types if the previous (1) or following (2) operation
    # has failed. We want all results to be NoNodeError or RuntimeInconsistency (which means, node
    # existance check wasn't performed, because node's parent is not there).
    types = NoNodeError, RuntimeInconsistency
    nodes_missing = [isinstance(result, types) for result in transaction.commit()]

    if not all(nodes_missing):
        logging.error("Aborting, some nodes already exist: %s" %
            ", ".join(node_list[idx] for idx, missing in enumerate(nodes_missing) if not missing)
        )

        return 1

    transaction = client.transaction()

    # TODO: Might be a good idea to set ACLs here so that these structural nodes are protected from
    # accidental deletions, but allow children modifications.
    [transaction.create(node) for node in node_list]

    # Operation results are either a string representing the created path or an exception object we
    # don't really care about.
    nodes_created = [result == node_list[idx] for idx, result in enumerate(transaction.commit())]

    if not all(nodes_created):
        logging.error("Aborting, couldn't create some nodes: %s" %
            ", ".join(node_list[idx] for idx, created in enumerate(nodes_created) if not created)
        )

        return 1

    logging.info("Cluster has been successfully bootstrapped, cluster id is: %s" % cluster_id)

if __name__ == "__main__":
    exit(main())
