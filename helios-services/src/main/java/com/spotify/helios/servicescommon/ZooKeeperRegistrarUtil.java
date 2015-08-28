/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.servicescommon;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.AgentInfo;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.HostInfo;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.Task;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.master.HostNotFoundException;
import com.spotify.helios.master.HostStillInUseException;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperOperation;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.collect.Lists.reverse;
import static com.spotify.helios.common.descriptors.Descriptor.parse;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.HostStatus.Status.DOWN;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.delete;
import static java.util.Collections.emptyMap;

public class ZooKeeperRegistrarUtil {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperRegistrarUtil.class);

  public static final Map<JobId, TaskStatus> EMPTY_STATUSES = emptyMap();
  public static final TypeReference<HostInfo>
      HOST_INFO_TYPE =
      new TypeReference<HostInfo>() {};
  public static final TypeReference<AgentInfo>
      AGENT_INFO_TYPE =
      new TypeReference<AgentInfo>() {};
  public static final TypeReference<Map<String, String>>
      STRING_MAP_TYPE =
      new TypeReference<Map<String, String>>() {};

  public static void registerHost(final ZooKeeperClient client, final String idPath,
                                  final String hostname, final String hostId)
      throws KeeperException {
    log.info("registering host: {}", hostname);

    // This would've been nice to do in a transaction but PathChildrenCache ensures paths
    // so we can't know what paths already exist so assembling a suitable transaction is too
    // painful.
    client.ensurePath(Paths.configHost(hostname));
    client.ensurePath(Paths.configHost(hostname));
    client.ensurePath(Paths.configHostJobs(hostname));
    client.ensurePath(Paths.configHostPorts(hostname));
    client.ensurePath(Paths.statusHost(hostname));
    client.ensurePath(Paths.statusHostJobs(hostname));

    // Finish registration by creating the id node last
    client.createAndSetData(idPath, hostId.getBytes(UTF_8));
  }

  public static void deregisterHost(final ZooKeeperClient client, final String host)
      throws HostNotFoundException, HostStillInUseException {
    log.info("deregistering host: {}", host);

    // TODO (dano): handle retry failures
    try {
      final List<ZooKeeperOperation> operations = Lists.newArrayList();

      // Remove all jobs deployed to this host
      final List<JobId> jobs = listHostJobs(client, host);

      if (jobs == null) {
        if (client.exists(Paths.configHost(host)) == null) {
          throw new HostNotFoundException("host [" + host + "] does not exist");
        }
      }

      if (jobs != null) {
        for (final JobId job : jobs) {
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
    } catch (NotEmptyException e) {
      final HostStatus hostStatus = getHostStatus(client, host);
      final List<JobId> jobs = hostStatus != null
                               ? ImmutableList.copyOf(hostStatus.getJobs().keySet())
                               : Collections.<JobId>emptyList();
      throw new HostStillInUseException(host, jobs);
    } catch (NoNodeException e) {
      throw new HostNotFoundException(host);
    } catch (KeeperException e) {
      throw new HeliosRuntimeException(e);
    }
  }

  public static List<JobId> listHostJobs(final ZooKeeperClient client, final String host) {
    final List<String> jobIdStrings;
    final String folder = Paths.statusHostJobs(host);
    try {
      jobIdStrings = client.getChildren(folder);
    } catch (KeeperException.NoNodeException e) {
      return null;
    } catch (KeeperException e) {
      throw new HeliosRuntimeException("List tasks for host failed: " + host, e);
    }
    final ImmutableList.Builder<JobId> jobIds = ImmutableList.builder();
    for (String jobIdString : jobIdStrings) {
      jobIds.add(JobId.fromString(jobIdString));
    }
    return jobIds.build();
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

  public static HostStatus getHostStatus(final ZooKeeperClient client, final String host) {
    final Stat stat;

    try {
      stat = client.exists(Paths.configHostId(host));
    } catch (KeeperException e) {
      throw new HeliosRuntimeException("Failed to check host status", e);
    }

    if (stat == null) {
      return null;
    }

    final boolean up = checkHostUp(client, host);
    final HostInfo hostInfo = getHostInfo(client, host);
    final AgentInfo agentInfo = getAgentInfo(client, host);
    final Map<JobId, Deployment> tasks = getTasks(client, host);
    final Map<JobId, TaskStatus> statuses = getTaskStatuses(client, host);
    final Map<String, String> environment = getEnvironment(client, host);

    return HostStatus.newBuilder()
        .setJobs(tasks)
        .setStatuses(fromNullable(statuses).or(EMPTY_STATUSES))
        .setHostInfo(hostInfo)
        .setAgentInfo(agentInfo)
        .setStatus(up ? UP : DOWN)
        .setEnvironment(environment)
        .build();
  }

  private static Map<String, String> getEnvironment(final ZooKeeperClient client,
                                                    final String host) {
    return tryGetEntity(client, Paths.statusHostEnvVars(host), STRING_MAP_TYPE, "environment");
  }

  private static AgentInfo getAgentInfo(final ZooKeeperClient client, final String host) {
    return tryGetEntity(client, Paths.statusHostAgentInfo(host), AGENT_INFO_TYPE, "agent info");
  }

  private static HostInfo getHostInfo(final ZooKeeperClient client, final String host) {
    return tryGetEntity(client, Paths.statusHostInfo(host), HOST_INFO_TYPE, "host info");
  }

  private static boolean checkHostUp(final ZooKeeperClient client, final String host) {
    try {
      final Stat stat = client.exists(Paths.statusHostUp(host));
      return stat != null;
    } catch (KeeperException e) {
      throw new HeliosRuntimeException("getting host " + host + " up status failed", e);
    }
  }

  private static Map<JobId, TaskStatus> getTaskStatuses(final ZooKeeperClient client,
                                                        final String host) {
    final Map<JobId, TaskStatus> statuses = Maps.newHashMap();
    final List<JobId> jobIds = ZooKeeperRegistrarUtil.listHostJobs(client, host);
    for (final JobId jobId : jobIds) {
      final TaskStatus status = getTaskStatus(client, host, jobId);
      if (status != null) {
        statuses.put(jobId, status);
      } else {
        log.debug("Task {} status missing for host {}", jobId, host);
      }
    }

    return statuses;
  }

  @Nullable
  public static TaskStatus getTaskStatus(final ZooKeeperClient client, final String host,
                                  final JobId jobId) {
    final String containerPath = Paths.statusHostJob(host, jobId);
    try {
      final byte[] data = client.getData(containerPath);
      return parse(data, TaskStatus.class);
    } catch (NoNodeException ignored) {
      return null;
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("Getting task " + jobId + " status " +
                                       "for host " + host + " failed", e);
    }
  }

  private static <T> T tryGetEntity(final ZooKeeperClient client, String path,
                                    TypeReference<T> type, String name) {
    try {
      final byte[] data = client.getData(path);
      return Json.read(data, type);
    } catch (NoNodeException e) {
      return null;
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("reading " + name + " info failed", e);
    }
  }

  private static Map<JobId, Deployment> getTasks(final ZooKeeperClient client, final String host) {
    final Map<JobId, Deployment> jobs = Maps.newHashMap();
    try {
      final String folder = Paths.configHostJobs(host);
      final List<String> jobIds;
      try {
        jobIds = client.getChildren(folder);
      } catch (KeeperException.NoNodeException e) {
        return null;
      }

      for (final String jobIdString : jobIds) {
        final JobId jobId = JobId.fromString(jobIdString);
        final String containerPath = Paths.configHostJob(host, jobId);
        try {
          final byte[] data = client.getData(containerPath);
          final Task task = parse(data, Task.class);
          jobs.put(jobId, Deployment.of(jobId, task.getGoal()));
        } catch (KeeperException.NoNodeException ignored) {
          log.debug("deployment config node disappeared: {}", jobIdString);
        }
      }
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("getting deployment config failed", e);
    }

    return jobs;
  }

}
