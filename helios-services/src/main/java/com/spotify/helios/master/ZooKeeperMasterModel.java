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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.AgentInfo;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.DeploymentGroupTasks;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.HostInfo;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.common.descriptors.RolloutTask;
import com.spotify.helios.common.descriptors.Task;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.rollingupdate.DefaultRolloutPlanner;
import com.spotify.helios.rollingupdate.DeploymentGroupEventFactory;
import com.spotify.helios.rollingupdate.DeploymentGroupEventFactory.RollingUpdateReason;
import com.spotify.helios.rollingupdate.RollingUpdateError;
import com.spotify.helios.rollingupdate.RollingUpdateOp;
import com.spotify.helios.rollingupdate.RollingUpdateOpFactory;
import com.spotify.helios.rollingupdate.RolloutPlanner;
import com.spotify.helios.servicescommon.KafkaRecord;
import com.spotify.helios.servicescommon.KafkaSender;
import com.spotify.helios.servicescommon.VersionedValue;
import com.spotify.helios.servicescommon.ZooKeeperRegistrarUtil;
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
import java.util.Objects;
import java.util.UUID;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.Descriptor.parse;
import static com.spotify.helios.common.descriptors.HostStatus.Status.DOWN;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.rollingupdate.DeploymentGroupEventFactory.RollingUpdateReason.HOSTS_CHANGED;
import static com.spotify.helios.rollingupdate.DeploymentGroupEventFactory.RollingUpdateReason.MANUAL;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.check;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.create;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.delete;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.set;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
  public static final TypeReference<List<String>>
      STRING_LIST_TYPE =
      new TypeReference<List<String>>() {};

  private static final String DEPLOYMENT_GROUP_EVENTS_KAFKA_TOPIC = "HeliosDeploymentGroupEvents";
  private static final DeploymentGroupEventFactory DEPLOYMENT_GROUP_EVENT_FACTORY =
      new DeploymentGroupEventFactory();

  private final ZooKeeperClientProvider provider;
  private final String name;
  private final KafkaSender kafkaSender;

  public  ZooKeeperMasterModel(final ZooKeeperClientProvider provider)
      throws IOException, InterruptedException {
    this(provider, null);
  }

  public ZooKeeperMasterModel(final ZooKeeperClientProvider provider, @Nullable final String name) {
    this(provider, name, null);
  }

  /**
   * Constructor
   * @param provider         {@link ZooKeeperClientProvider}
   * @param name             The hostname of the machine running the {@link MasterModel}
   * @param kafkaSender      {@link KafkaSender}
   */
  public ZooKeeperMasterModel(
      final ZooKeeperClientProvider provider,
      @Nullable final String name,
      @Nullable final KafkaSender kafkaSender) {
    this.provider = provider;
    this.name = name;
    this.kafkaSender = kafkaSender;
  }

  /**
   * Registers a host into ZooKeeper.  The {@code id} is initially generated randomly by the Agent
   * and persisted on disk.  This way, in the event that you have two agents attempting to register
   * with the same value of @{code host}, the first one will win.
   */
  @Override
  public void registerHost(final String host, final String id) {
    final ZooKeeperClient client = provider.get("registerHost");
    try {
      ZooKeeperRegistrarUtil.registerHost(client, Paths.configHostId(host), host, id);
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
    final ZooKeeperClient client = provider.get("deregisterHost");
    ZooKeeperRegistrarUtil.deregisterHost(client, host);
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
    } catch (NoNodeException e) {
      throw new HeliosRuntimeException("adding job " + job + " failed due to missing ZK path: " +
                                       e.getPath(), e);
    } catch (final KeeperException e) {
      throw new HeliosRuntimeException("adding job " + job + " failed", e);
    }
  }

  /**
   * Given a jobId, returns the N most recent events in its history in the cluster.
   */
  @Override
  public List<TaskStatusEvent> getJobHistory(final JobId jobId) throws JobDoesNotExistException {
    return getJobHistory(jobId, null);
  }

  /**
   * Given a jobId and host, returns the N most recent events in its history on that host in the
   * cluster.
   */
  @Override
  public List<TaskStatusEvent> getJobHistory(final JobId jobId, final String host)
      throws JobDoesNotExistException {
    final Job descriptor = getJob(jobId);
    if (descriptor == null) {
      throw new JobDoesNotExistException(jobId);
    }
    final ZooKeeperClient client = provider.get("getJobHistory");
    final List<String> hosts;

    try {
      hosts = (!isNullOrEmpty(host)) ? singletonList(host) :
              client.getChildren(Paths.historyJobHosts(jobId));
    } catch (NoNodeException e) {
      return emptyList();
    } catch (KeeperException e) {
      throw Throwables.propagate(e);
    }

    final List<TaskStatusEvent> jsEvents = Lists.newArrayList();

    for (String h : hosts) {
      final List<String> events;
      try {
        events = client.getChildren(Paths.historyJobHostEvents(jobId, h));
      } catch (NoNodeException e) {
        continue;
      } catch (KeeperException e) {
        throw Throwables.propagate(e);
      }

      for (String event : events) {
        try {
          byte[] data = client.getData(Paths.historyJobHostEventsTimestamp(
              jobId, h, Long.valueOf(event)));
          final TaskStatus status = Json.read(data, TaskStatus.class);
          jsEvents.add(new TaskStatusEvent(status, Long.valueOf(event), h));
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

    try {
      try {
        client.ensurePath(Paths.configDeploymentGroups());
        client.ensurePath(Paths.statusDeploymentGroups());
        client.transaction(
            create(Paths.configDeploymentGroup(deploymentGroup.getName()), deploymentGroup),
            create(Paths.statusDeploymentGroup(deploymentGroup.getName())),
            create(Paths.statusDeploymentGroupHosts(deploymentGroup.getName()))
          );
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
      client.delete(Paths.configDeploymentGroup(name));
      if (client.exists(Paths.statusDeploymentGroupHosts(name)) != null) {
        client.delete(Paths.statusDeploymentGroupHosts(name));
      }
      if (client.exists(Paths.statusDeploymentGroup(name)) != null) {
        client.delete(Paths.statusDeploymentGroup(name));
      }
    } catch (final NoNodeException e) {
      throw new DeploymentGroupDoesNotExistException(name);
    } catch (final KeeperException e) {
      throw new HeliosRuntimeException("removing deployment-group " + name + " failed", e);
    }
  }

  @Override
  public void updateDeploymentGroupHosts(final String name, final List<String> hosts)
      throws DeploymentGroupDoesNotExistException {
    log.debug("updating deployment-group hosts: name={}", name);
    final ZooKeeperClient client = provider.get("updateDeploymentGroupHosts");
    try {
      Optional<Integer> version = Optional.absent();
      List<String> curHosts;
      try {
        final Node node = client.getNode(Paths.statusDeploymentGroupHosts(name));
        version = Optional.of(node.getStat().getVersion());
        curHosts = Json.read(node.getBytes(), new TypeReference<List<String>>() {});
      } catch (NoNodeException | JsonMappingException e) {
        curHosts = Collections.emptyList();
      }

      if (!version.isPresent() || !hosts.equals(curHosts)) {
        // Node not present or hosts have changed
        final List<ZooKeeperOperation> ops = Lists.newArrayList();
        ops.add(set(Paths.statusDeploymentGroupHosts(name), Json.asBytes(hosts)));

        client.ensurePath(Paths.statusDeploymentGroup(name));
        client.ensurePath(Paths.statusDeploymentGroupTasks(name));
        final DeploymentGroup deploymentGroup = getDeploymentGroup(name);
        ImmutableList<Map<String, Object>> events = ImmutableList.of();

        if (deploymentGroup.getJobId() != null) {
          final DeploymentGroupStatus deploymentGroupStatus = getDeploymentGroupStatus(name);
          if (deploymentGroupStatus == null || deploymentGroupStatus.getState() != FAILED) {
            final RollingUpdateOp op =
                getInitRollingUpdateOps(deploymentGroup, hosts, HOSTS_CHANGED);
            ops.addAll(op.operations());
            events = op.events();
          }
        }

        client.transaction(ops);

        if (kafkaSender != null) {
          for (final Map<String, Object> event : events) {
            kafkaSender.send(KafkaRecord.of(
                DEPLOYMENT_GROUP_EVENTS_KAFKA_TOPIC, Json.asBytesUnchecked(event)));
          }
        }
      }
    } catch (NoNodeException e) {
      throw new DeploymentGroupDoesNotExistException(name, e);
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("updating deployment group hosts failed", e);
    }
  }

  @Override
  public void rollingUpdate(final DeploymentGroup deploymentGroup,
                            final JobId jobId,
                            final RolloutOptions options)
      throws DeploymentGroupDoesNotExistException, JobDoesNotExistException {
    checkNotNull(deploymentGroup, "deploymentGroup");

    log.info("rolling-update on deployment-group: name={}", deploymentGroup.getName());

    final DeploymentGroup updated = deploymentGroup.toBuilder()
        .setJobId(jobId)
        .setRolloutOptions(options)
        .build();

    if (getJob(jobId) == null) {
      throw new JobDoesNotExistException(jobId);
    }

    final List<ZooKeeperOperation> operations = Lists.newArrayList();
    final ZooKeeperClient client = provider.get("rollingUpdate");

    operations.add(set(Paths.configDeploymentGroup(updated.getName()), updated));
    final RollingUpdateOp op = getInitRollingUpdateOps(updated, MANUAL);
    operations.addAll(op.operations());

    try {
      client.ensurePath(Paths.statusDeploymentGroup(updated.getName()));
      client.ensurePath(Paths.statusDeploymentGroupTasks(updated.getName()));
      client.transaction(operations);

      if (kafkaSender != null) {
        for (final Map<String, Object> event : op.events()) {
          kafkaSender.send(KafkaRecord.of(
              DEPLOYMENT_GROUP_EVENTS_KAFKA_TOPIC, Json.asBytesUnchecked(event)));
        }
      }
    } catch (final NoNodeException e) {
      throw new DeploymentGroupDoesNotExistException(deploymentGroup.getName());
    } catch (final KeeperException e) {
      throw new HeliosRuntimeException(
          "rolling-update on deployment-group " + deploymentGroup.getName() + " failed", e);
    }
  }

  private RollingUpdateOp getInitRollingUpdateOps(final DeploymentGroup deploymentGroup,
                                                  final RollingUpdateReason reason)
      throws DeploymentGroupDoesNotExistException {
    final List<String> hosts = getDeploymentGroupHosts(deploymentGroup.getName());
    return getInitRollingUpdateOps(deploymentGroup, hosts, reason);
  }

  private RollingUpdateOp getInitRollingUpdateOps(final DeploymentGroup deploymentGroup,
                                                  final List<String> hosts,
                                                  final RollingUpdateReason reason) {
    final Map<String, HostStatus> hostsAndStatuses = Maps.newLinkedHashMap();
    for (final String host : hosts) {
      hostsAndStatuses.put(host, getHostStatus(host));
    }

    final RolloutPlanner rolloutPlanner = DefaultRolloutPlanner.of(deploymentGroup);
    final List<RolloutTask> rolloutTasks = rolloutPlanner.plan(hostsAndStatuses);
    final DeploymentGroupTasks tasks = DeploymentGroupTasks.newBuilder()
        .setRolloutTasks(rolloutTasks)
        .setTaskIndex(0)
        .setDeploymentGroup(deploymentGroup)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        tasks, DEPLOYMENT_GROUP_EVENT_FACTORY);
    return opFactory.start(deploymentGroup, reason);
  }

  private Map<String, VersionedValue<DeploymentGroupTasks>> getDeploymentGroupTasks(
      final ZooKeeperClient client) {
    final String folder = Paths.statusDeploymentGroupTasks();
    try {
      final List<String> names;
      try {
        names = client.getChildren(folder);
      } catch (NoNodeException e) {
        return Collections.emptyMap();
      }

      final Map<String, VersionedValue<DeploymentGroupTasks>> ret = Maps.newHashMap();
      for (final String name : names) {
        final String path = Paths.statusDeploymentGroupTasks(name);
        try {
          final Node node = client.getNode(path);
          final byte[] data = node.getBytes();
          final int version = node.getStat().getVersion();
          if (data.length == 0) {
            // This can happen because of ensurePath creates an empty node
            log.debug("Ignoring empty deployment group tasks {}", name);
          } else {
            final DeploymentGroupTasks val = parse(data, DeploymentGroupTasks.class);
            ret.put(name, VersionedValue.of(val, version));
          }
        } catch (NoNodeException e) {
          // Ignore, the deployment group was deleted before we had a chance to read it.
          log.debug("Ignoring deleted deployment group tasks {}", name);
        }
      }
      return ret;
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("getting deployment group tasks failed", e);
    }
  }

  private RollingUpdateOp processRollingUpdateTask(final ZooKeeperClient client,
                                                   final RollingUpdateOpFactory opFactory,
                                                   final RolloutTask task,
                                                   final DeploymentGroup deploymentGroup) {
    final RolloutTask.Action action = task.getAction();
    final String host = task.getTarget();

    switch (action) {
      case UNDEPLOY_OLD_JOBS:
        // add undeploy ops for jobs previously deployed by this deployment group
        return rollingUpdateUndeploy(client, opFactory, deploymentGroup, host);
      case DEPLOY_NEW_JOB:
        // add deploy ops for the new job
        return rollingUpdateDeploy(client, opFactory, deploymentGroup, host);
      case AWAIT_RUNNING:
        return rollingUpdateAwaitRunning(client, opFactory, deploymentGroup, host);
      default:
        throw new HeliosRuntimeException(String.format(
            "unknown rollout task type %s for deployment group %s.",
            action, deploymentGroup.getName()));
    }
  }

  @Override
  public void rollingUpdateStep() {
    final ZooKeeperClient client = provider.get("rollingUpdateStep");

    final Map<String, VersionedValue<DeploymentGroupTasks>> tasksMap =
        getDeploymentGroupTasks(client);

    for (Map.Entry<String, VersionedValue<DeploymentGroupTasks>> entry : tasksMap.entrySet()) {
      final String deploymentGroupName = entry.getKey();
      final VersionedValue<DeploymentGroupTasks> versionedTasks = entry.getValue();
      final DeploymentGroupTasks tasks = versionedTasks.value();

      log.debug("rolling-update step on deployment-group: name={}", deploymentGroupName);

      try {
        final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
            tasks, DEPLOYMENT_GROUP_EVENT_FACTORY);
        final RolloutTask task = tasks.getRolloutTasks().get(tasks.getTaskIndex());
        final RollingUpdateOp op = processRollingUpdateTask(
            client, opFactory, task, tasks.getDeploymentGroup());

        if (!op.operations().isEmpty()) {
          final List<ZooKeeperOperation> ops = Lists.newArrayList();
          ops.add(check(Paths.statusDeploymentGroupTasks(deploymentGroupName),
                        versionedTasks.version()));
          ops.addAll(op.operations());
          try {
            client.transaction(ops);

            // Emit events
            if (kafkaSender != null) {
              for (final Map<String, Object> event : op.events()) {
                kafkaSender.send(KafkaRecord.of(
                    DEPLOYMENT_GROUP_EVENTS_KAFKA_TOPIC, Json.asBytesUnchecked(event)));
              }
            }
          } catch (KeeperException.BadVersionException e) {
            // some other master beat us in processing this rolling update step. not exceptional.
            // ideally we would check the path in the exception, but curator doesn't provide a path
            // for exceptions thrown as part of a transaction.
            log.debug("error saving rolling-update operations: {}", e);
          } catch (KeeperException e) {
            log.error("rolling-update on deployment-group {} failed", deploymentGroupName, e);
          }
        }
      } catch (final Exception e) {
        log.error("error processing rolling update step for {}", deploymentGroupName, e);
      }
    }
  }

  private RollingUpdateOp rollingUpdateAwaitRunning(final ZooKeeperClient client,
                                                    final RollingUpdateOpFactory opFactory,
                                                    final DeploymentGroup deploymentGroup,
                                                    final String host) {
    final TaskStatus taskStatus =
        ZooKeeperRegistrarUtil.getTaskStatus(client, host, deploymentGroup.getJobId());
    final JobId jobId = deploymentGroup.getJobId();

    if (taskStatus == null) {
      // Handle cases where agent has not written job status to zookeeper.

      // If job is not listed under /config/hosts node, it may have been deployed successfully and
      // then manually undeployed. The job will not get redeployed, so treat this as a failure.
      final Deployment deployment = getDeployment(host, jobId);
      if (deployment == null) {
        return opFactory.error(
            "Job unexpectedly undeployed. Perhaps it was manually undeployed?", host,
            RollingUpdateError.JOB_UNEXPECTEDLY_UNDEPLOYED);
      }

      // Check if we've exceeded the timeout for the rollout operation.
      if (isRolloutTimedOut(client, deploymentGroup)) {
        return opFactory.error("timed out while retrieving job status", host,
                               RollingUpdateError.TIMED_OUT_RETRIEVING_JOB_STATUS);
      }

      // We haven't detected any errors, so assume the agent will write the status soon.
      return opFactory.yield();
    } else if (!taskStatus.getState().equals(TaskStatus.State.RUNNING)) {
      // job isn't running yet

      if (isRolloutTimedOut(client, deploymentGroup)) {
        // We exceeded the configured deploy timeout, and this job is still not running
        final List<TaskStatus.State> previousJobStates = getPreviousJobStates(jobId, host, 10);
        final Map<String, Object> metadata = Maps.newHashMap();
        metadata.put("jobState", taskStatus.getState());
        metadata.put("previousJobStates", previousJobStates);
        return opFactory.error("timed out waiting for job to reach RUNNING", host,
                               RollingUpdateError.TIMED_OUT_WAITING_FOR_JOB_TO_REACH_RUNNING,
                               metadata);
      }

      return opFactory.yield();
    } else {
      // the job is running on the host. last thing we have to ensure is that it was
      // deployed by this deployment group. otherwise some weird conflict has occurred and we
      // won't be able to undeploy the job on the next update.
      final Deployment deployment = getDeployment(host, deploymentGroup.getJobId());
      if (deployment == null) {
        return opFactory.error(
            "deployment for this job not found in zookeeper. " +
            "Perhaps it was manually undeployed?", host,
            RollingUpdateError.JOB_UNEXPECTEDLY_UNDEPLOYED);
      } else if (!Objects.equals(deployment.getDeploymentGroupName(), deploymentGroup.getName())) {
        return opFactory.error(
            "job was already deployed, either manually or by a different deployment group", host,
            RollingUpdateError.JOB_ALREADY_DEPLOYED);
      }

      return opFactory.nextTask();
    }
  }

  private boolean isRolloutTimedOut(final ZooKeeperClient client,
                                    final DeploymentGroup deploymentGroup) {
    try {
      final String statusPath = Paths.statusDeploymentGroupTasks(deploymentGroup.getName());
      final long secondsSinceDeploy = MILLISECONDS.toSeconds(
          System.currentTimeMillis() - client.getNode(statusPath).getStat().getMtime());
      return secondsSinceDeploy > deploymentGroup.getRolloutOptions().getTimeout();
    } catch (KeeperException e) {
      // statusPath doesn't exist or some other ZK issue. probably this deployment group
      // was removed.
      log.warn("error determining deployment group modification time: {} - {}",
               deploymentGroup.getName(), e);
      return false;
    }
  }

  private RollingUpdateOp rollingUpdateDeploy(final ZooKeeperClient client,
                                              final RollingUpdateOpFactory opFactory,
                                              final DeploymentGroup deploymentGroup,
                                              final String host) {
    final Deployment deployment = Deployment.of(deploymentGroup.getJobId(), Goal.START,
                                                Deployment.EMTPY_DEPLOYER_USER, this.name,
                                                deploymentGroup.getName());

    try {
      final String token =
          firstNonNull(deploymentGroup.getRolloutOptions().getToken(), Job.EMPTY_TOKEN);
      return opFactory.nextTask(getDeployOperations(client, host, deployment, token));
    } catch (JobDoesNotExistException e) {
      return opFactory.error(e, host, RollingUpdateError.JOB_NOT_FOUND);
    } catch (TokenVerificationException e) {
      return opFactory.error(e, host, RollingUpdateError.TOKEN_VERIFICATION_ERROR);
    } catch (HostNotFoundException e) {
      return opFactory.error(e, host, RollingUpdateError.HOST_NOT_FOUND);
    } catch (JobPortAllocationConflictException e) {
      return opFactory.error(e, host, RollingUpdateError.PORT_CONFLICT);
    } catch (JobAlreadyDeployedException e) {
      // Nothing to do
      return opFactory.nextTask();
    }
  }

  private RollingUpdateOp rollingUpdateUndeploy(final ZooKeeperClient client,
                                                final RollingUpdateOpFactory opFactory,
                                                final DeploymentGroup deploymentGroup,
                                                final String host) {
    final List<ZooKeeperOperation> operations = Lists.newArrayList();

    for (final Deployment deployment : getTasks(client, host).values()) {
      final boolean isOwnedByDeploymentGroup = Objects.equals(
          deployment.getDeploymentGroupName(), deploymentGroup.getName());
      final boolean isSameJob = deployment.getJobId().equals(deploymentGroup.getJobId());
      final RolloutOptions rolloutOptions = deploymentGroup.getRolloutOptions();

      if (isOwnedByDeploymentGroup || (
          isSameJob && rolloutOptions.getMigrate())) {
        if (isSameJob && isOwnedByDeploymentGroup && deployment.getGoal().equals(Goal.START)) {
          // The job we want deployed is already deployed and set to run, so just leave it.
          continue;
        }

        try {
          final String token =
              firstNonNull(deploymentGroup.getRolloutOptions().getToken(), Job.EMPTY_TOKEN);
          operations.addAll(getUndeployOperations(client, host, deployment.getJobId(), token));
        } catch (TokenVerificationException e) {
          return opFactory.error(e, host, RollingUpdateError.TOKEN_VERIFICATION_ERROR);
        } catch (HostNotFoundException e) {
          return opFactory.error(e, host, RollingUpdateError.HOST_NOT_FOUND);
        } catch (JobNotDeployedException e) {
          // probably somebody beat us to the punch of undeploying. that's fine.
        }
      }
    }

    return opFactory.nextTask(operations);
  }

  @Override
  public void stopDeploymentGroup(final String deploymentGroupName)
      throws DeploymentGroupDoesNotExistException {
    checkNotNull(deploymentGroupName, "name");

    log.info("stop deployment-group: name={}", deploymentGroupName);

    final ZooKeeperClient client = provider.get("stopDeploymentGroup");

    // TODO(staffan): This is stupid, but required for correct behaviour right now.
    final DeploymentGroup deploymentGroup = getDeploymentGroup(deploymentGroupName);

    // Delete deployment group tasks (if any) and set DG state to FAILED
    final DeploymentGroupStatus status = DeploymentGroupStatus.newBuilder()
        .setState(FAILED)
        .setError("Stopped by user")
        .build();
    final String statusPath = Paths.statusDeploymentGroup(deploymentGroupName);
    final String tasksPath = Paths.statusDeploymentGroupTasks(deploymentGroupName);

    try {
      client.ensurePath(statusPath);
      client.ensurePath(Paths.statusDeploymentGroupTasks());

      // NOTE: This is racey. If a rollout finishes before the delete() is executed
      // then this will fail. This is annoying for users, but at least means we won't have
      // inconsistent state. There's also another race in case the DG is inactive when stop is
      // called, but has become active when we execute the ZK transaction to stop the DG.
      final Stat tasksStat = client.stat(tasksPath);
      if (tasksStat != null) {
        client.transaction(set(statusPath, status),
                           delete(tasksPath));
      } else {
        // There doesn't seem to be a "check that node doesn't exist" operation so we
        // do a create and a delete on the same path to emulate it.
        client.transaction(set(statusPath, status),
                           create(tasksPath),
                           delete(tasksPath));
      }
    } catch (final NoNodeException e) {
      throw new DeploymentGroupDoesNotExistException(deploymentGroupName);
    } catch (final KeeperException e) {
      throw new HeliosRuntimeException(
          "stop deployment-group " + deploymentGroupName + " failed", e);
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

      final byte[] bytes = node.getBytes();
      if (bytes.length == 0) {
        return null;
      }

      return Json.read(bytes, DeploymentGroupStatus.class);
    } catch (NoNodeException e) {
      return null;
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("getting deployment group status " + name + " failed", e);
    }
  }

  @Override
  public List<String> getDeploymentGroupHosts(final String name)
      throws DeploymentGroupDoesNotExistException {
    log.debug("getting deployment group hosts: {}", name);
    final ZooKeeperClient client = provider.get("getDeploymentGroupHosts");

    final DeploymentGroup deploymentGroup = getDeploymentGroup(client, name);
    if (deploymentGroup == null) {
      throw new DeploymentGroupDoesNotExistException(name);
    }

    try {
      final byte[] data = client.getData(Paths.statusDeploymentGroupHosts(name));

      if (data.length > 0) {
        return Json.read(data, STRING_LIST_TYPE);
      }
    } catch (NoNodeException e) {
      // not fatal
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("reading deployment group hosts failed: " + name, e);
    }

    return emptyList();
  }

  /**
   * Returns the job configuration for the job specified by {@code id} as a
   * {@link Job} object.
   */
  @Override
  public Job getJob(final JobId id) {
    log.debug("getting job: {}", id);
    final ZooKeeperClient client = provider.get("getJobId");
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
      final TaskStatus taskStatus = ZooKeeperRegistrarUtil.getTaskStatus(client, host, jobId);
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
      if (Paths.isConfigJobCreation(child)) {
        return Paths.configJobCreationId(child);
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
    deployJobRetry(client, host, deployment, 0, token);
  }

  private void deployJobRetry(final ZooKeeperClient client, final String host,
                              final Deployment deployment, int count, final String token)
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

    final Task task = new Task(job, deployment.getGoal(), deployment.getDeployerUser(),
                               deployment.getDeployerMaster(), deployment.getDeploymentGroupName());
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
      deployJobRetry(client, host, deployment, count + 1, token);
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
        checkForPortConflicts(client, host, port, id);
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
    final Deployment existingDeployment = getDeployment(host, jobId);

    if (job == null) {
      throw new JobNotDeployedException(host, jobId);
    }
    verifyToken(token, job);

    assertHostExists(client, host);
    assertTaskExists(client, host, deployment.getJobId());

    final String path = Paths.configHostJob(host, jobId);
    final Task task = new Task(job, deployment.getGoal(),
                               existingDeployment.getDeployerUser(),
                               existingDeployment.getDeployerMaster(),
                               existingDeployment.getDeploymentGroupName());
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
      return Deployment.of(jobId, task.getGoal(), task.getDeployerUser(), task.getDeployerMaster(),
                           task.getDeploymentGroupName());
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
    final Map<JobId, TaskStatus> statuses = ZooKeeperRegistrarUtil.getTaskStatuses(client, host);
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
          jobs.put(jobId, Deployment.of(jobId, task.getGoal(), task.getDeployerUser(),
                                        task.getDeployerMaster(), task.getDeploymentGroupName()));
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

  private List<ZooKeeperOperation> getUndeployOperations(final ZooKeeperClient client,
                                                         final String host,
                                                         final JobId jobId,
                                                         final String token)
      throws HostNotFoundException, JobNotDeployedException, TokenVerificationException {
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

      return ImmutableList.of(delete(nodes));

    } catch (NoNodeException e) {
      // This method is racy since it's possible someone undeployed the job after we called
      // getDeployment and checked the job exists. If we now discover the job is undeployed,
      // throw an exception and handle it the same as if we discovered this earlier.
      throw new JobNotDeployedException(host, jobId);
    } catch (KeeperException e) {
      throw new HeliosRuntimeException("calculating undeploy operations failed", e);
    }
  }

  private List<ZooKeeperOperation> getDeployOperations(final ZooKeeperClient client,
                                                       final String host,
                                                       final Deployment deployment,
                                                       final String token)
      throws JobDoesNotExistException, JobAlreadyDeployedException, TokenVerificationException,
             HostNotFoundException, JobPortAllocationConflictException {
    assertHostExists(client, host);
    final JobId id = deployment.getJobId();
    final Job job = getJob(id);

    if (job == null) {
      throw new JobDoesNotExistException(id);
    }
    verifyToken(token, job);

    final UUID operationId = UUID.randomUUID();
    final String jobPath = Paths.configJob(id);
    final String taskPath = Paths.configHostJob(host, id);
    final String taskCreationPath = Paths.configHostJobCreation(host, id, operationId);

    final List<Integer> staticPorts = staticPorts(job);
    final Map<String, byte[]> portNodes = Maps.newHashMap();
    final byte[] idJson = id.toJsonBytes();
    for (final int port : staticPorts) {
      final String path = Paths.configHostPort(host, port);
      portNodes.put(path, idJson);
    }

    final Task task = new Task(job, deployment.getGoal(), deployment.getDeployerUser(),
                               deployment.getDeployerMaster(), deployment.getDeploymentGroupName());
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
      // Check for port collisions after checking whether the job is already deployed to the host.
      // This is to prevent us from telling the user a misleading error message about port conflicts
      // if the real reason of the failure is that the job is already deployed.
      for (final int port : staticPorts) {
        checkForPortConflicts(client, host, port, id);
      }
      operations.add(create(taskPath, task));
      operations.add(create(taskCreationPath));
    } catch (KeeperException e) {
      throw new HeliosRuntimeException("reading existing task description failed", e);
    }

    return ImmutableList.copyOf(operations);
  }

  private static void verifyToken(final String token, final Job job)
      throws TokenVerificationException {
    checkNotNull(token, "token");
    if (!token.equals(job.getToken())) {
      throw new TokenVerificationException(job.getId());
    }
  }

  private static void
  checkForPortConflicts(final ZooKeeperClient client, final String host, final int port,
                        final JobId jobId) throws JobPortAllocationConflictException {
    try {
      final String path = Paths.configHostPort(host, port);
      if (client.stat(path) == null) {
        return;
      }
      final byte[] b = client.getData(path);
      final JobId existingJobId = parse(b, JobId.class);
      throw new JobPortAllocationConflictException(jobId, existingJobId, host, port);
    } catch (KeeperException | IOException ex) {
      throw new HeliosRuntimeException("checking port allocations failed", ex);
    }
  }

  private List<TaskStatus.State> getPreviousJobStates(final JobId jobId,
                                                      final String host,
                                                      final int maxStates) {
    List<TaskStatus.State> previousStates;
    try {
      final List<TaskStatusEvent> jobHistory = getJobHistory(jobId, host);
      jobHistory.subList(0, Math.min(maxStates, jobHistory.size()));
      Function<TaskStatusEvent, TaskStatus.State> statusesToStrings =
          new Function<TaskStatusEvent, TaskStatus.State>() {
            @Override
            public TaskStatus.State apply(@Nullable TaskStatusEvent input) {
              if (input != null) {
                return input.getStatus().getState();
              }
              return null;
            }
          };
      previousStates = Lists.transform(jobHistory, statusesToStrings);
    } catch (JobDoesNotExistException ignored) {
      previousStates = emptyList();
    }

    return previousStates;
  }
}
