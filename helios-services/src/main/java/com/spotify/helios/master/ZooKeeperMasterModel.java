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

package com.spotify.helios.master;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.AgentInfo;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.HostInfo;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.Task;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.servicescommon.coordination.Node;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClientProvider;
import com.spotify.helios.servicescommon.coordination.ZooKeeperOperation;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static com.spotify.helios.common.descriptors.Descriptor.parse;
import static com.spotify.helios.common.descriptors.HostStatus.Status.DOWN;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.check;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.create;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.delete;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.set;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * The Helios Master's view into ZooKeeper.
 */
public class ZooKeeperMasterModel implements MasterModel {
  private static final Comparator<TaskStatusEvent> EVENT_COMPARATOR =
      new Comparator<TaskStatusEvent>() {
        @Override
        public int compare(TaskStatusEvent arg0, TaskStatusEvent arg1) {
          if (arg1.getTimestamp() > arg0.getTimestamp()) {
            return -1;
          } else if (arg1.getTimestamp() == arg0.getTimestamp()) {
            return 0;
          } else {
            return 1;
          }
        }
      };

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperMasterModel.class);

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

  private final ZooKeeperClientProvider provider;

  public ZooKeeperMasterModel(final ZooKeeperClientProvider provider) {
    this.provider = provider;
  }

  /**
   * Registers a host into ZooKeeper.  The {@code id} is initially generated randomly by the Agent
   * and persisted on disk.  This way, in the event that you have two agents attempting to register
   * with the same value of @{code host}, the first one will win.
   */
  @Override
  public void registerHost(final String host, final String id) {
    log.info("registering host: {}", host);
    final ZooKeeperClient client = provider.get("registerHost");
    try {
      // TODO (dano): this code is replicated in AgentZooKeeperRegistrar

      // This would've been nice to do in a transaction but PathChildrenCache ensures paths
      // so we can't know what paths already exist so assembling a suitable transaction is too
      // painful.
      client.ensurePath(Paths.configHost(host));
      client.ensurePath(Paths.configHostJobs(host));
      client.ensurePath(Paths.configHostPorts(host));
      client.ensurePath(Paths.statusHost(host));
      client.ensurePath(Paths.statusHostJobs(host));

      // Finish registration by creating the id node last
      client.createAndSetData(Paths.configHostId(host), id.getBytes(UTF_8));
    } catch (Exception e) {
      throw new HeliosRuntimeException("registering host " + host + " failed", e);
    }
  }

  /**
   * Returns a list of the hosts/agents that have been registered.
   */
  @Override
  public List<String> listHosts() {
    try {
      // TODO (dano): only return hosts whose agents completed registration (i.e. has id nodes)
      return provider.get("listHosts").getChildren(Paths.configHosts());
    } catch (KeeperException.NoNodeException e) {
      return emptyList();
    } catch (KeeperException e) {
      throw new HeliosRuntimeException("listing hosts failed", e);
    }
  }

  /**
   * Returns a list of the host names of the currently running masters.
   */
  @Override
  public List<String> getRunningMasters() {
    final ZooKeeperClient client = provider.get("getRunningMasters");
    try {
      final List<String> masters = client.getChildren(Paths.statusMaster());
      final ImmutableList.Builder<String> upMasters = ImmutableList.builder();
      for (final String master : masters) {
        if (client.exists(Paths.statusMasterUp(master)) != null) {
          upMasters.add(master);
        }
      }
      return upMasters.build();
    } catch (KeeperException e) {
      throw new HeliosRuntimeException("listing masters failed", e);
    }
  }

  /**
   * Undoes the effect of {@link ZooKeeperMasterModel#registerHost(String, String)}.  Cleans up
   * any leftover host-related things.
   */
  @Override
  public void deregisterHost(final String host)
      throws HostNotFoundException, HostStillInUseException {
    log.info("deregistering host: {}", host);
    final ZooKeeperClient client = provider.get("deregisterHost");
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
      final HostStatus hostStatus = getHostStatus(host);
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

  private List<String> safeGetChildren(final ZooKeeperClient client, final String path) {
    try {
      return client.getChildren(path);
    } catch (KeeperException ignore) {
      return ImmutableList.of();
    }
  }

  private List<String> safeListRecursive(final ZooKeeperClient client, final String path)
      throws KeeperException {
    try {
      return client.listRecursive(path);
    } catch (NoNodeException e) {
      return ImmutableList.of();
    }
  }

  /**
   * Adds a job into the configuration.
   */
  @Override
  public void addJob(final Job job) throws JobExistsException {
    log.info("adding job: {}", job);
    final JobId id = job.getId();
    final UUID operationId = UUID.randomUUID();
    final String creationPath = Paths.configJobCreation(id, operationId);
    final ZooKeeperClient client = provider.get("addJob");
    try {
      try {
        client.ensurePath(Paths.historyJob(id));
        client.transaction(create(Paths.configJob(id), job),
                           create(Paths.configJobRefShort(id), id),
                           create(Paths.configJobHosts(id)),
                           create(creationPath),
                           // Touch the jobs root node so that its version is bumped on every job
                           // change down the tree. Effectively, make it that version == cVersion.
                           set(Paths.configJobs(), UUID.randomUUID().toString().getBytes()));
      } catch (final NodeExistsException e) {
        if (client.exists(creationPath) != null) {
          // The job was created, we're done here
          return;
        }
        throw new JobExistsException(id.toString());
      }
    } catch (final KeeperException e) {
      throw new HeliosRuntimeException("adding job " + job + " failed", e);
    }
  }

  /**
   * Given a jobId, returns the N most recent events in it's history in the cluster.
   */
  @Override
  public List<TaskStatusEvent> getJobHistory(final JobId jobId) throws JobDoesNotExistException {
    final Job descriptor = getJob(jobId);
    if (descriptor == null) {
      throw new JobDoesNotExistException(jobId);
    }
    final ZooKeeperClient client = provider.get("getJobHistory");
    final List<String> hosts;
    try {
      hosts = client.getChildren(Paths.historyJobHosts(jobId));
    } catch (NoNodeException e) {
      return emptyList();
    } catch (KeeperException e) {
      throw Throwables.propagate(e);
    }

    final List<TaskStatusEvent> jsEvents = Lists.newArrayList();

    for (String host : hosts) {
      final List<String> events;
      try {
        events = client.getChildren(Paths.historyJobHostEvents(jobId, host));
      } catch (KeeperException e) {
        throw Throwables.propagate(e);
      }

      for (String event : events) {
        try {
          byte[] data = client.getData(Paths.historyJobHostEventsTimestamp(
              jobId, host, Long.valueOf(event)));
          final TaskStatus status = Json.read(data, TaskStatus.class);
          jsEvents.add(new TaskStatusEvent(status, Long.valueOf(event), host));
        } catch (NoNodeException e) { // ignore, it went away before we read it
        } catch (KeeperException | IOException e) {
          throw Throwables.propagate(e);
        }
      }
    }

    return Ordering.from(EVENT_COMPARATOR).sortedCopy(jsEvents);
  }

  @Override
  public void addDeploymentGroup(final DeploymentGroup deploymentGroup)
      throws DeploymentGroupExistsException {
    log.info("adding deployment-group: {}", deploymentGroup);
    final ZooKeeperClient client = provider.get("addDeploymentGroup");

    final DeploymentGroupStatus initialStatus = DeploymentGroupStatus.newBuilder()
        .setDeploymentGroup(deploymentGroup)
        .setState(DeploymentGroupStatus.State.QUEUED)
        .setIndex(0)
        .setHosts(EMPTY_LIST)
        .setVersion(0)
        .build();

    try {
      try {
        client.ensurePath(Paths.configDeploymentGroups());
        client.ensurePath(Paths.statusDeploymentGroups());

        client.transaction(
            create(Paths.configDeploymentGroup(deploymentGroup.getName()), deploymentGroup),
            create(Paths.statusDeploymentGroup(deploymentGroup.getName()), initialStatus));
      } catch (final NodeExistsException e) {
        throw new DeploymentGroupExistsException(deploymentGroup.getName());
      }
    } catch (final KeeperException e) {
      throw new HeliosRuntimeException("adding deployment-group " + deploymentGroup + " failed", e);
    }
  }

  @Override
  public DeploymentGroup getDeploymentGroup(final String name)
      throws DeploymentGroupDoesNotExistException {
    log.debug("getting deployment-group: {}", name);
    final ZooKeeperClient client = provider.get("getDeploymentGroup");
    return getDeploymentGroup(client, name);
  }

  private DeploymentGroup getDeploymentGroup(final ZooKeeperClient client, final String name)
      throws DeploymentGroupDoesNotExistException {
    try {
      final byte[] data = client.getData(Paths.configDeploymentGroup(name));
      return Json.read(data, DeploymentGroup.class);
    } catch (NoNodeException e) {
      throw new DeploymentGroupDoesNotExistException(name);
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("getting deployment-group " + name + " failed", e);
    }
  }

  @Override
  public void removeDeploymentGroup(final String name) throws DeploymentGroupDoesNotExistException {
    log.info("removing deployment-group: name={}", name);
    final ZooKeeperClient client = provider.get("removeDeploymentGroup");
    try {
      client.ensurePath(Paths.configDeploymentGroups());
      client.deleteRecursive(Paths.configDeploymentGroup(name));
    } catch (final NoNodeException e) {
      throw new DeploymentGroupDoesNotExistException(name);
    } catch (final KeeperException e) {
      throw new HeliosRuntimeException("removing deployment-group " + name + " failed", e);
    }
  }

  @Override
  public void rollingUpdate(final String deploymentGroupName, final JobId jobId)
      throws DeploymentGroupDoesNotExistException, JobDoesNotExistException {
    log.info("rolling-update on deployment-group: name={}", deploymentGroupName);

    final DeploymentGroup existing = getDeploymentGroup(deploymentGroupName);
    final Job job = getJob(jobId);
    if (job == null) {
      throw new JobDoesNotExistException(jobId);
    }

    final ZooKeeperClient client = provider.get("updateDeploymentGroup");

    final DeploymentGroup updated = existing.toBuilder().setJob(jobId).build();

    try {
      client.ensurePath(Paths.configDeploymentGroups());
      client.transaction(
          set(Paths.configDeploymentGroup(deploymentGroupName), updated));
    } catch (final NoNodeException e) {
      throw new DeploymentGroupDoesNotExistException(deploymentGroupName);
    } catch (final KeeperException e) {
      throw new HeliosRuntimeException(
          "rolling-update on deployment-group " + deploymentGroupName + " failed", e);
    }
  }

  /**
   * Returns a {@link Map} of deployment group name to {@link DeploymentGroup} objects for all of
   * the deployment groups known.
   */
  @Override
  public Map<String, DeploymentGroup> getDeploymentGroups() {
    log.debug("getting deployment groups");
    final String folder = Paths.configDeploymentGroups();
    final ZooKeeperClient client = provider.get("getDeploymentGroups");
    try {
      final List<String> names;
      try {
        names = client.getChildren(folder);
      } catch (NoNodeException e) {
        return Maps.newHashMap();
      }
      final Map<String, DeploymentGroup> descriptors = Maps.newHashMap();
      for (final String name : names) {
        final String path = Paths.configDeploymentGroup(name);
        try {
          final byte[] data = client.getData(path);
          final DeploymentGroup descriptor = parse(data, DeploymentGroup.class);
          descriptors.put(descriptor.getName(), descriptor);
        } catch (NoNodeException e) {
          // Ignore, the deployment group was deleted before we had a chance to read it.
          log.debug("Ignoring deleted deployment group {}", name);
        }
      }
      return descriptors;
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("getting deployment groups failed", e);
    }
  }

  @Override
  public DeploymentGroupStatus getDeploymentGroupStatus(final String name)
      throws DeploymentGroupDoesNotExistException {
    log.debug("getting deployment group status: {}", name);
    final ZooKeeperClient client = provider.get("getDeploymentGroupStatus");

    final DeploymentGroup deploymentGroup = getDeploymentGroup(client, name);
    if (deploymentGroup == null) {
      return null;
    }

    try {
      final Node node = client.getNode(Paths.statusDeploymentGroup(name));
      final DeploymentGroupStatus status = Json.read(node.getBytes(), DeploymentGroupStatus.class);
      return status.asBuilder()
          .setVersion(node.getStat().getVersion())
          .build();
    } catch (NoNodeException e) {
      throw new DeploymentGroupDoesNotExistException(name);
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("getting deployment group status " + name + " failed", e);
    }
  }

  /**
   * Returns the job configuration for the job specified by {@code id} as a
   * {@link Job} object.
   */
  @Override
  public Job getJob(final JobId id) {
    log.debug("getting job: {}", id);
    final ZooKeeperClient client = provider.get("getJob");
    return getJob(client, id);
  }


  private Job getJob(final ZooKeeperClient client, final JobId id) {
    final String path = Paths.configJob(id);
    try {
      final byte[] data = client.getData(path);
      return Json.read(data, Job.class);
    } catch (NoNodeException e) {
      // Return null to indicate that the job does not exist
      return null;
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("getting job " + id + " failed", e);
    }
  }

  /**
   * Returns a {@link Map} of {@link JobId} to {@link Job} objects for all of the jobs known.
   */
  @Override
  public Map<JobId, Job> getJobs() {
    log.debug("getting jobs");
    final String folder = Paths.configJobs();
    final ZooKeeperClient client = provider.get("getJobs");
    try {
      final List<String> ids;
      try {
        ids = client.getChildren(folder);
      } catch (NoNodeException e) {
        return Maps.newHashMap();
      }
      final Map<JobId, Job> descriptors = Maps.newHashMap();
      for (final String id : ids) {
        final JobId jobId = JobId.fromString(id);
        final String path = Paths.configJob(jobId);
        try {
          final byte[] data = client.getData(path);
          final Job descriptor = parse(data, Job.class);
          descriptors.put(descriptor.getId(), descriptor);
        } catch (NoNodeException e) {
          // Ignore, the job was deleted before we had a chance to read it.
          log.debug("Ignoring deleted job {}", jobId);
        }
      }
      return descriptors;
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("getting jobs failed", e);
    }
  }

  /**
   * Returns the current job status as a {@link JobStatus} object.
   */
  @Override
  public JobStatus getJobStatus(final JobId jobId) {
    final ZooKeeperClient client = provider.get("getJobStatus");

    final Job job = getJob(client, jobId);
    if (job == null) {
      return null;
    }

    final List<String> hosts;
    try {
      hosts = listJobHosts(client, jobId);
    } catch (JobDoesNotExistException e) {
      return null;
    }

    final ImmutableMap.Builder<String, Deployment> deployments = ImmutableMap.builder();
    final ImmutableMap.Builder<String, TaskStatus> taskStatuses = ImmutableMap.builder();
    for (final String host : hosts) {
      final TaskStatus taskStatus = getTaskStatus(client, host, jobId);
      if (taskStatus != null) {
        taskStatuses.put(host, taskStatus);
      }
      final Deployment deployment = getDeployment(host, jobId);
      if (deployment != null) {
        deployments.put(host, deployment);
      }
    }

    final Map<String, Deployment> deploymentsMap = deployments.build();
    return JobStatus.newBuilder()
        .setJob(job)
        .setDeployments(deploymentsMap)
        .setTaskStatuses(taskStatuses.build())
        .build();
  }

  private List<String> listJobHosts(final ZooKeeperClient client, final JobId jobId)
      throws JobDoesNotExistException {
    final List<String> hosts;
    try {
      hosts = client.getChildren(Paths.configJobHosts(jobId));
    } catch (NoNodeException e) {
      throw new JobDoesNotExistException(jobId);
    } catch (KeeperException e) {
      throw new HeliosRuntimeException("failed to list hosts for job: " + jobId, e);
    }
    return hosts;
  }

  @Override
  public Job removeJob(JobId jobId) throws JobDoesNotExistException, JobStillDeployedException {
    try {
      return removeJob(jobId, Job.EMPTY_TOKEN);
    } catch (TokenVerificationException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Deletes a job from ZooKeeper.  Ensures that job is not currently running anywhere.
   */
  @Override
  public Job removeJob(final JobId id, final String token)
      throws JobDoesNotExistException, JobStillDeployedException, TokenVerificationException {
    log.info("removing job: id={}", id);
    final ZooKeeperClient client = provider.get("removeJob");
    final Job job = getJob(client, id);
    if (job == null) {
      throw new JobDoesNotExistException(id);
    }
    verifyToken(token, job);

    // TODO (dano): handle retry failures
    try {
      final ImmutableList.Builder<ZooKeeperOperation> operations = ImmutableList.builder();
      final UUID jobCreationOperationId = getJobCreation(client, id);
      if (jobCreationOperationId != null) {
        operations.add(delete(Paths.configJobCreation(id, jobCreationOperationId)));
      }
      operations.add(delete(Paths.configJobHosts(id)),
                     delete(Paths.configJobRefShort(id)),
                     delete(Paths.configJob(id)),
                     // Touch the jobs root node so that its version is bumped on every job
                     // change down the tree. Effectively, make it that version == cVersion.
                     set(Paths.configJobs(), UUID.randomUUID().toString().getBytes()));
      client.transaction(operations.build());
    } catch (final NoNodeException e) {
      throw new JobDoesNotExistException(id);
    } catch (final NotEmptyException e) {
      throw new JobStillDeployedException(id, listJobHosts(client, id));
    } catch (final KeeperException e) {
      throw new HeliosRuntimeException("removing job " + id + " failed", e);
    }

    return job;
  }

  private UUID getJobCreation(final ZooKeeperClient client, final JobId id)
      throws KeeperException {
    final String parent = Paths.configHostJobCreationParent(id);
    final List<String> children = client.getChildren(parent);
    for (final String child : children) {
      if (Paths.isConfigJobCreation(id, parent, child)) {
        return Paths.configJobCreationId(id, parent, child);
      }
    }
    return null;
  }

  @Override
  public void deployJob(String host, Deployment job)
      throws HostNotFoundException, JobAlreadyDeployedException, JobDoesNotExistException,
             JobPortAllocationConflictException {
    try {
      deployJob(host, job, Job.EMPTY_TOKEN);
    } catch (TokenVerificationException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a config entry within the specified agent to un/deploy a job, or more generally, change
   * the deployment status according to the {@code Goal} value in {@link Deployment}.
   */
  @Override
  public void deployJob(final String host, final Deployment deployment, final String token)
      throws JobDoesNotExistException, JobAlreadyDeployedException, HostNotFoundException,
             JobPortAllocationConflictException, TokenVerificationException {
    final ZooKeeperClient client = provider.get("deployJob");
    deployJobRetry(client, host, deployment, 0, token, null);
  }

  // TODO(drewc): this kinda screams "long method"
  private void deployJobRetry(final ZooKeeperClient client, final String host,
                              final Deployment deployment, int count, final String token,
                              final DeploymentGroup deploymentGroup)
      throws JobDoesNotExistException, JobAlreadyDeployedException, HostNotFoundException,
             JobPortAllocationConflictException, TokenVerificationException {
    if (count == 3) {
      throw new HeliosRuntimeException("3 failures (possibly concurrent modifications) while " +
                                       "deploying. Giving up.");
    }
    log.info("deploying {}: {} (retry={})", deployment, host, count);

    final JobId id = deployment.getJobId();
    final Job job = getJob(id);

    if (job == null) {
      throw new JobDoesNotExistException(id);
    }
    verifyToken(token, job);

    final UUID operationId = UUID.randomUUID();
    final String jobPath = Paths.configJob(id);

    try {
      Paths.configHostJob(host, id);
    } catch (IllegalArgumentException e) {
      throw new HostNotFoundException("Could not find Helios host '" + host + "'");
    }

    final String taskPath = Paths.configHostJob(host, id);
    final String taskCreationPath = Paths.configHostJobCreation(host, id, operationId);

    final List<Integer> staticPorts = staticPorts(job);
    final Map<String, byte[]> portNodes = Maps.newHashMap();
    final byte[] idJson = id.toJsonBytes();
    for (final int port : staticPorts) {
      final String path = Paths.configHostPort(host, port);
      portNodes.put(path, idJson);
    }

    final Task task = new Task(job, deployment.getGoal(), deployment.getDeployerUser());
    final List<ZooKeeperOperation> operations = Lists.newArrayList(
        check(jobPath),
        create(portNodes),
        create(Paths.configJobHost(id, host)));

    // Attempt to read a task here.
    try {
      client.getNode(taskPath);
      // if we get here the node exists already
      throw new JobAlreadyDeployedException(host, id);
    } catch (NoNodeException e) {
      operations.add(create(taskPath, task));
      operations.add(create(taskCreationPath));
    } catch (KeeperException e) {
      throw new HeliosRuntimeException("reading existing task description failed", e);
    }

    // TODO (dano): Failure handling is racy wrt agent and job modifications.
    try {
      client.transaction(operations);
      log.info("deployed {}: {} (retry={})", deployment, host, count);
    } catch (NoNodeException e) {
      // Either the job, the host or the task went away
      assertJobExists(client, id);
      assertHostExists(client, host);
      // If the job and host still exists, we likely tried to redeploy a job that had an UNDEPLOY
      // goal and lost the race with the agent removing the task before we could set it. Retry.
      deployJobRetry(client, host, deployment, count + 1, token, deploymentGroup);
    } catch (NodeExistsException e) {
      // Check for conflict due to transaction retry
      try {
        if (client.exists(taskCreationPath) != null) {
          // Our creation operation node existed, we're done here
          return;
        }
      } catch (KeeperException ex) {
        throw new HeliosRuntimeException("checking job deployment failed", ex);
      }
      try {
        // Check if the job was already deployed
        if (client.stat(taskPath) != null) {
          throw new JobAlreadyDeployedException(host, id);
        }
      } catch (KeeperException ex) {
        throw new HeliosRuntimeException("checking job deployment failed", e);
      }

      // Check for static port collisions
      for (final int port : staticPorts) {
        final String path = Paths.configHostPort(host, port);
        try {
          if (client.stat(path) == null) {
            continue;
          }
          final byte[] b = client.getData(path);
          final JobId existingJobId = parse(b, JobId.class);
          throw new JobPortAllocationConflictException(id, existingJobId, host, port);
        } catch (KeeperException | IOException ex) {
          throw new HeliosRuntimeException("checking port allocations failed", e);
        }
      }

      // Catch all for logic and ephemeral issues
      throw new HeliosRuntimeException("deploying job failed", e);
    } catch (KeeperException e) {
      throw new HeliosRuntimeException("deploying job failed", e);
    }
  }

  private void assertJobExists(final ZooKeeperClient client, final JobId id)
      throws JobDoesNotExistException {
    try {
      final String path = Paths.configJob(id);
      if (client.stat(path) == null) {
        throw new JobDoesNotExistException(id);
      }
    } catch (KeeperException e) {
      throw new HeliosRuntimeException("checking job existence failed", e);
    }
  }

  private List<Integer> staticPorts(final Job job) {
    final List<Integer> staticPorts = Lists.newArrayList();
    for (final PortMapping portMapping : job.getPorts().values()) {
      if (portMapping.getExternalPort() != null) {
        staticPorts.add(portMapping.getExternalPort());
      }
    }
    return staticPorts;
  }

  @Override
  public void updateDeployment(String host, Deployment deployment)
      throws HostNotFoundException, JobNotDeployedException {
    try {
      updateDeployment(host, deployment, Job.EMPTY_TOKEN);
    } catch (TokenVerificationException e) {
      Throwables.propagate(e);
    }
  }

  /**
   * Used to update the existing deployment of a job.
   */
  @Override
  public void updateDeployment(final String host, final Deployment deployment, final String token)
      throws HostNotFoundException, JobNotDeployedException, TokenVerificationException {
    log.info("updating deployment {}: {}", deployment, host);

    final ZooKeeperClient client = provider.get("updateDeployment");

    final JobId jobId = deployment.getJobId();
    final Job job = getJob(client, jobId);

    if (job == null) {
      throw new JobNotDeployedException(host, jobId);
    }
    verifyToken(token, job);

    assertHostExists(client, host);
    assertTaskExists(client, host, deployment.getJobId());

    final String path = Paths.configHostJob(host, jobId);
    final Task task = new Task(job, deployment.getGoal(), Task.EMPTY_DEPLOYER_USER);
    try {
      client.setData(path, task.toJsonBytes());
    } catch (Exception e) {
      throw new HeliosRuntimeException("updating deployment " + deployment +
                                       " on host " + host + " failed", e);
    }
  }

  private void assertHostExists(final ZooKeeperClient client, final String host)
  throws HostNotFoundException {
    try {
      client.getData(Paths.configHost(host));
    } catch (NoNodeException e) {
      throw new HostNotFoundException(host, e);
    } catch (KeeperException e) {
      throw new HeliosRuntimeException(e);
    }
  }

  private void assertTaskExists(final ZooKeeperClient client, final String host, final JobId jobId)
  throws JobNotDeployedException {
    try {
      client.getData(Paths.configHostJob(host, jobId));
    } catch (NoNodeException e) {
      throw new JobNotDeployedException(host, jobId);
    } catch (KeeperException e) {
      throw new HeliosRuntimeException(e);
    }
  }

  /**
   * Returns the current deployment state of {@code jobId} on {@code host}.
   */
  @Override
  public Deployment getDeployment(final String host, final JobId jobId) {
    final String path = Paths.configHostJob(host, jobId);
    final ZooKeeperClient client = provider.get("getDeployment");
    try {
      final byte[] data = client.getData(path);
      final Task task = parse(data, Task.class);
      return Deployment.of(jobId, task.getGoal(), task.getDeployerUser());
    } catch (KeeperException.NoNodeException e) {
      return null;
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("getting deployment failed", e);
    }
  }

  /**
   * Returns the current status of the host named by {@code host}.
   */
  @Override
  public HostStatus getHostStatus(final String host) {
    final Stat stat;
    final ZooKeeperClient client = provider.get("getHostStatus");

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
    final Map<String, String> labels = getLabels(client, host);

    return HostStatus.newBuilder()
        .setJobs(tasks)
        .setStatuses(fromNullable(statuses).or(EMPTY_STATUSES))
        .setHostInfo(hostInfo)
        .setAgentInfo(agentInfo)
        .setStatus(up ? UP : DOWN)
        .setEnvironment(environment)
        .setLabels(labels)
        .build();
  }

  private <T> T tryGetEntity(final ZooKeeperClient client, String path, TypeReference<T> type,
                             String name) {
    try {
      final byte[] data = client.getData(path);
      return Json.read(data, type);
    } catch (NoNodeException e) {
      return null;
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("reading " + name + " info failed", e);
    }
  }

  private Map<String, String> getEnvironment(final ZooKeeperClient client, final String host) {
    return tryGetEntity(client, Paths.statusHostEnvVars(host), STRING_MAP_TYPE, "environment");
  }

  private Map<String, String> getLabels(final ZooKeeperClient client, final String host) {
    return tryGetEntity(client, Paths.statusHostLabels(host), STRING_MAP_TYPE, "labels");
  }

  private AgentInfo getAgentInfo(final ZooKeeperClient client, final String host) {
    return tryGetEntity(client, Paths.statusHostAgentInfo(host), AGENT_INFO_TYPE, "agent info");
  }

  private HostInfo getHostInfo(final ZooKeeperClient client, final String host) {
    return tryGetEntity(client, Paths.statusHostInfo(host), HOST_INFO_TYPE, "host info");
  }

  private boolean checkHostUp(final ZooKeeperClient client, final String host) {
    try {
      final Stat stat = client.exists(Paths.statusHostUp(host));
      return stat != null;
    } catch (KeeperException e) {
      throw new HeliosRuntimeException("getting host " + host + " up status failed", e);
    }
  }

  private Map<JobId, TaskStatus> getTaskStatuses(final ZooKeeperClient client, final String host) {
    final Map<JobId, TaskStatus> statuses = Maps.newHashMap();
    final List<JobId> jobIds = listHostJobs(client, host);
    for (final JobId jobId : jobIds) {
      TaskStatus status;
      try {
        status = getTaskStatus(client, host, jobId);
      } catch (HeliosRuntimeException e) {
        // Skip this task status so we can return other available information instead of failing the
        // entire thing.
        status = null;
      }

      if (status != null) {
        statuses.put(jobId, status);
      } else {
        log.debug("Task {} status missing for host {}", jobId, host);
      }
    }

    return statuses;
  }

  private List<JobId> listHostJobs(final ZooKeeperClient client, final String host) {
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

  @Nullable
  private TaskStatus getTaskStatus(final ZooKeeperClient client, final String host,
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

  private Map<JobId, Deployment> getTasks(final ZooKeeperClient client, final String host) {
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

  @Override
  public Deployment undeployJob(String host, JobId jobId)
      throws HostNotFoundException, JobNotDeployedException {
    try {
      return undeployJob(host, jobId, Job.EMPTY_TOKEN);
    } catch (TokenVerificationException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Undeploys the job specified by {@code jobId} on {@code host}.
   */
  @Override
  public Deployment undeployJob(final String host, final JobId jobId, final String token)
      throws HostNotFoundException, JobNotDeployedException, TokenVerificationException {
    log.info("undeploying {}: {}", jobId, host);
    final ZooKeeperClient client = provider.get("undeployJob");

    assertHostExists(client, host);

    final Deployment deployment = getDeployment(host, jobId);
    if (deployment == null) {
      throw new JobNotDeployedException(host, jobId);
    }

    final Job job = getJob(client, jobId);
    verifyToken(token, job);
    final String configHostJobPath = Paths.configHostJob(host, jobId);

    try {
      // use listRecursive to remove both job node and its child creation node
      final List<String> nodes = newArrayList(reverse(client.listRecursive(configHostJobPath)));
      nodes.add(Paths.configJobHost(jobId, host));

      final List<Integer> staticPorts = staticPorts(job);
      for (int port : staticPorts) {
          nodes.add(Paths.configHostPort(host, port));
      }

      client.transaction(delete(nodes));

    } catch (NoNodeException e) {
      // This method is racy since it's possible someone undeployed the job after we called
      // getDeployment and checked the job exists. If we now discover the job is undeployed,
      // throw an exception and handle it the same as if we discovered this earlier.
      throw new JobNotDeployedException(host, jobId);
    } catch (KeeperException e) {
      throw new HeliosRuntimeException("Removing deployment failed", e);
    }
    return deployment;
  }

  private static void verifyToken(final String token, final Job job)
      throws TokenVerificationException {
    checkNotNull(token, "token");
    if (!token.equals(job.getToken())) {
      throw new TokenVerificationException(job.getId());
    }
  }
}
