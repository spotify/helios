/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.servicescommon;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Lists.reverse;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.check;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.create;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.delete;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.master.HostNotFoundException;
import com.spotify.helios.master.HostStillInUseException;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperOperation;
import java.util.List;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperRegistrarUtil {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperRegistrarUtil.class);

  public static boolean isHostRegistered(final ZooKeeperClient client, final String host) {
    try {
      final Stat stat = client.exists(Paths.configHostId(host));
      return stat != null;
    } catch (KeeperException e) {
      throw new HeliosRuntimeException("getting host " + host + " id failed", e);
    }
  }

  public static void registerHost(final ZooKeeperClient client, final String idPath,
                                  final String hostname, final String hostId)
      throws KeeperException {
    log.info("registering host: {}", hostname);

    // This would've been nice to do in a transaction but PathChildrenCache ensures paths
    // so we can't know what paths already exist so assembling a suitable transaction is too
    // painful.
    client.ensurePath(Paths.configHost(hostname));
    client.ensurePath(Paths.configHostJobs(hostname));
    client.ensurePath(Paths.configHostPorts(hostname));
    client.ensurePath(Paths.statusHost(hostname));
    client.ensurePath(Paths.statusHostJobs(hostname));

    // Finish registration by creating the id node last
    client.createAndSetData(idPath, hostId.getBytes(UTF_8));
  }

  /**
   * Re-register an agent with a different host id. Will remove the existing status of the agent
   * but preserve any jobs deployed to the host and their history.
   *
   * @param client ZooKeeperClient
   * @param host   Host
   * @param hostId ID of the host
   *
   * @throws HostNotFoundException If the hostname we are trying to re-register as doesn't exist.
   * @throws KeeperException       If an unexpected zookeeper error occurs.
   */
  public static void reRegisterHost(final ZooKeeperClient client,
                                    final String host, final String hostId)
      throws HostNotFoundException, KeeperException {
    // * Delete everything in the /status/hosts/<hostname> subtree
    // * Don't delete any history for the job (on the host)
    // * DON'T touch anything in the /config/hosts/<hostname> subtree, except updating the host id
    log.info("re-registering host: {}, new host id: {}", host, hostId);
    try {
      final List<ZooKeeperOperation> operations = Lists.newArrayList();

      // Check that the host exists in ZK
      operations.add(check(Paths.configHost(host)));

      // Remove the host status
      final List<String> nodes = safeListRecursive(client, Paths.statusHost(host));
      for (final String node : reverse(nodes)) {
        operations.add(delete(node));
      }

      // ...and re-create the /status/hosts/<host>/jobs node + parent
      operations.add(create(Paths.statusHost(host)));
      operations.add(create(Paths.statusHostJobs(host)));

      // Update the host ID
      // We don't have WRITE permissions to the node, so delete and re-create it.
      operations.add(delete(Paths.configHostId(host)));
      operations.add(create(Paths.configHostId(host), hostId.getBytes(UTF_8)));

      client.transaction(operations);
    } catch (NoNodeException e) {
      throw new HostNotFoundException(host);
    } catch (KeeperException e) {
      throw new HeliosRuntimeException(e);
    }
  }

  public static void deregisterHost(final ZooKeeperClient client, final String host)
      throws HostNotFoundException, HostStillInUseException {
    log.info("deregistering host: {}", host);

    // TODO (dano): handle retry failures
    try {
      final List<ZooKeeperOperation> operations = Lists.newArrayList();

      if (client.exists(Paths.configHost(host)) == null) {
        throw new HostNotFoundException("host [" + host + "] does not exist");
      }

      // Remove all jobs deployed to this host
      final List<String> jobs = safeGetChildren(client, Paths.configHostJobs(host));

      for (final String jobString : jobs) {
        final JobId job = JobId.fromString(jobString);
        final String hostJobPath = Paths.configHostJob(host, job);

        final List<String> nodes = safeListRecursive(client, hostJobPath);
        for (final String node : reverse(nodes)) {
          operations.add(delete(node));
        }
        if (client.exists(Paths.configJobHost(job, host)) != null) {
          operations.add(delete(Paths.configJobHost(job, host)));
        }
        // Clean out the history for each job
        final List<String> history = safeListRecursive(client, Paths.historyJobHost(job, host));
        for (final String s : reverse(history)) {
          operations.add(delete(s));
        }
      }

      operations.add(delete(Paths.configHostJobs(host)));

      // Remove the host status
      final List<String> nodes = safeListRecursive(client, Paths.statusHost(host));
      for (final String node : reverse(nodes)) {
        operations.add(delete(node));
      }

      // Remove port allocations
      final List<String> ports = safeGetChildren(client, Paths.configHostPorts(host));
      for (final String port : ports) {
        operations.add(delete(Paths.configHostPort(host, Integer.valueOf(port))));
      }
      operations.add(delete(Paths.configHostPorts(host)));

      // Remove host id
      final String idPath = Paths.configHostId(host);
      if (client.exists(idPath) != null) {
        operations.add(delete(idPath));
      }

      // Remove host config root
      operations.add(delete(Paths.configHost(host)));

      client.transaction(operations);
    } catch (NoNodeException e) {
      throw new HostNotFoundException(host);
    } catch (KeeperException e) {
      throw new HeliosRuntimeException(e);
    }
  }

  private static List<String> safeGetChildren(final ZooKeeperClient client, final String path) {
    try {
      return client.getChildren(path);
    } catch (KeeperException ignore) {
      return ImmutableList.of();
    }
  }

  private static List<String> safeListRecursive(final ZooKeeperClient client, final String path)
      throws KeeperException {
    try {
      return client.listRecursive(path);
    } catch (NoNodeException e) {
      return ImmutableList.of();
    }
  }
}
