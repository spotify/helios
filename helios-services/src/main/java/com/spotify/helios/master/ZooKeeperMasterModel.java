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

package com.spotify.helios.master;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static com.spotify.helios.common.descriptors.DeploymentGroup.RollingUpdateReason.HOSTS_CHANGED;
import static com.spotify.helios.common.descriptors.DeploymentGroup.RollingUpdateReason.MANUAL;
import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.ROLLING_OUT;
import static com.spotify.helios.common.descriptors.Descriptor.parse;
import static com.spotify.helios.common.descriptors.HostStatus.Status.DOWN;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.check;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.create;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.delete;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.set;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
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
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.rollingupdate.DeploymentGroupEventFactory;
import com.spotify.helios.rollingupdate.RollingUndeployPlanner;
import com.spotify.helios.rollingupdate.RollingUpdateError;
import com.spotify.helios.rollingupdate.RollingUpdateOp;
import com.spotify.helios.rollingupdate.RollingUpdateOpFactory;
import com.spotify.helios.rollingupdate.RollingUpdatePlanner;
import com.spotify.helios.servicescommon.EventSender;
import com.spotify.helios.servicescommon.VersionedValue;
import com.spotify.helios.servicescommon.ZooKeeperRegistrarUtil;
import com.spotify.helios.servicescommon.coordination.Node;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClientProvider;
import com.spotify.helios.servicescommon.coordination.ZooKeeperOperation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final DeploymentGroupEventFactory DEPLOYMENT_GROUP_EVENT_FACTORY =
      new DeploymentGroupEventFactory();

  private final ZooKeeperClientProvider provider;
  private final String name;
  private final List<EventSender> eventSenders;
  private final String deploymentGroupEventTopic;

  /**
   * Constructor.
   *
   * @param provider     {@link ZooKeeperClientProvider}
   * @param name         The hostname of the machine running the {@link MasterModel}
   * @param eventSenders {@link EventSender}
   */
  public ZooKeeperMasterModel(final ZooKeeperClientProvider provider,
                              final String name,
                              final List<EventSender> eventSenders,
                              final String deploymentGroupEventTopic) {
    this.provider = Preconditions.checkNotNull(provider);
    this.name = Preconditions.checkNotNull(name);
    this.eventSenders = Preconditions.checkNotNull(eventSenders);
    this.deploymentGroupEventTopic = deploymentGroupEventTopic;
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

  @Override
  public List<String> listHosts(final String namePatternFilter) {
    Preconditions.checkNotNull(namePatternFilter, "namePatternFilter");
    final Predicate<String> matchesPattern = Pattern.compile(namePatternFilter).asPredicate();
    return listHosts().stream()
        .filter(matchesPattern)
        .collect(Collectors.toList());
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
      throw new HeliosRuntimeException("adding job " + job + " failed due to missing ZK path: "
                                       + e.getPath(), e);
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
      throw new RuntimeException(e);
    }

    final List<TaskStatusEvent> jsEvents = Lists.newArrayList();

    for (final String h : hosts) {
      final List<String> events;
      try {
        events = client.getChildren(Paths.historyJobHostEvents(jobId, h));
      } catch (NoNodeException e) {
        continue;
      } catch (KeeperException e) {
        throw new RuntimeException(e);
      }

      for (final String event : events) {
        try {
          final byte[] data = client.getData(Paths.historyJobHostEventsTimestamp(
              jobId, h, Long.valueOf(event)));
          final TaskStatus status = Json.read(data, TaskStatus.class);
          jsEvents.add(new TaskStatusEvent(status, Long.valueOf(event), h));
        } catch (NoNodeException e) { // ignore, it went away before we read it
        } catch (KeeperException | IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    return Ordering.from(EVENT_COMPARATOR).sortedCopy(jsEvents);
  }

  /**
   * Create a deployment group.
   *
   * <p>If successful, the following ZK nodes will be created:
   * <ul>
   * <li>/config/deployment-groups/[group-name]</li>
   * <li>/status/deployment-groups/[group-name]</li>
   * <li>/status/deployment-groups/[group-name]/hosts</li>
   * </ul>
   * These nodes are guaranteed to exist until the DG is removed.
   *
   * <p>If the operation fails no ZK nodes will be created. If any of the nodes above already exist
   * the operation will fail.
   *
   * @throws DeploymentGroupExistsException If a DG with the same name already exists.
   */
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
            create(Paths.statusDeploymentGroupHosts(deploymentGroup.getName()),
                Json.asBytesUnchecked(emptyList())),
            create(Paths.statusDeploymentGroupRemovedHosts(deploymentGroup.getName()),
                Json.asBytesUnchecked(emptyList()))
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

  /**
   * Remove a deployment group.
   *
   * <p>If successful, all ZK nodes associated with the DG will be deleted. Specifically these
   * nodes are guaranteed to be non-existent after a successful remove (not all of them might exist
   * before, though):
   * <ul>
   * <li>/config/deployment-groups/[group-name]</li>
   * <li>/status/deployment-groups/[group-name]</li>
   * <li>/status/deployment-groups/[group-name]/hosts</li>
   * <li>/status/deployment-groups/[group-name]/removed</li>
   * <li>/status/deployment-group-tasks/[group-name]</li>
   * </ul>
   * If the operation fails no ZK nodes will be removed.
   *
   * @throws DeploymentGroupDoesNotExistException If the DG does not exist.
   */
  @Override
  public void removeDeploymentGroup(final String name) throws DeploymentGroupDoesNotExistException {
    log.info("removing deployment-group: name={}", name);
    final ZooKeeperClient client = provider.get("removeDeploymentGroup");
    try {
      client.ensurePath(Paths.configDeploymentGroups());
      client.ensurePath(Paths.statusDeploymentGroups());
      client.ensurePath(Paths.statusDeploymentGroupTasks());

      final List<ZooKeeperOperation> operations = Lists.newArrayList();

      final List<String> paths = ImmutableList.of(
          Paths.configDeploymentGroup(name),
          Paths.statusDeploymentGroup(name),
          Paths.statusDeploymentGroupHosts(name),
          Paths.statusDeploymentGroupRemovedHosts(name),
          Paths.statusDeploymentGroupTasks(name)
      );

      // For each deployment group path:
      //
      // * If it exists: delete it.
      // * If it doesn't exist, add and delete it in the same transaction. This is a round-about
      //   way of ensuring that it wasn't created when we commit the transaction.
      //
      // This is particularly important for /status/deployment-group-tasks/[group-name], which
      // might exist if a rolling-update is in progress. To avoid inconsistent state we make sure
      // it's deleted if it does exist.
      //
      // Having /status/deployment-group-tasks/[group-name] for removed groups around will cause
      // DGs to become slower and spam logs with errors so we want to avoid it.
      for (final String path : paths) {
        if (client.exists(path) == null) {
          operations.add(create(path));
        }
      }
      for (final String path : Lists.reverse(paths)) {
        operations.add(delete(path));
      }

      client.transaction(operations);
    } catch (final NoNodeException e) {
      throw new DeploymentGroupDoesNotExistException(name);
    } catch (final KeeperException e) {
      throw new HeliosRuntimeException("removing deployment-group " + name + " failed", e);
    }
  }

  /**
   * Determines whether we should allow deployment group hosts to be updated.
   *
   * <p>We don't want deployment groups that are ROLLING_OUT to change hosts in order to avoid the
   * following race:
   *
   * <p>- Manual rolling update A of a bad, broken job is triggered.
   * - The hosts change before rolling update A completes, triggering rolling update B.
   * - Rolling update B fails, because it inherited the bad, broken job from update A.
   * - The hosts change again, and we trigger unwanted rolling update C because the the previous
   *   rolling update (B) had reason=HOSTS_CHANGED.
   *
   * <p>It's safe to simply abort the hosts changed operation as updateOnHostsChange will be called
   * again by a reactor.
   *
   * @param status The status of the deployment group attempting to change hosts.
   *
   * @return True if it's safe to update the hosts.
   */
  private boolean allowHostChange(final DeploymentGroupStatus status) {
    if (status == null) {
      // We're in an unknown state. Go hog wild.
      return true;
    }
    return status.getState() != ROLLING_OUT;
  }

  /**
   * Determines whether we should trigger a rolling update when deployment group hosts change.
   *
   * <p>We want to avoid triggering an automatic rolling update if the most recent rolling update
   * was triggered manually, and failed.
   *
   * @param group  The deployment group that is changing hosts.
   * @param status The status of the aforementioned deployment group.
   *
   * @return True if we should perform a rolling update.
   */
  private boolean updateOnHostChange(final DeploymentGroup group,
                                     final DeploymentGroupStatus status) {
    if (status == null) {
      // We're in an unknown state. Go hog wild.
      return true;
    }

    if (group.getRollingUpdateReason() == null) {
      // The last rolling update didn't fail, so there's no reason to expect this one will.
      return status.getState() != FAILED;
    }

    if (group.getRollingUpdateReason() == HOSTS_CHANGED && status.getState() == FAILED) {
      // The last rolling update failed, but it was triggered by hosts changing. Try again.
      return true;
    }

    // The last rolling update didn't fail, so there's no reason to expect this one will.
    return status.getState() != FAILED;
  }

  private List<String> removedHosts(final List<String> currentHosts,
                                    final List<String> newHosts,
                                    final List<String> previouslyRemovedHosts) {
    final Set<String> ch = ImmutableSet.copyOf(currentHosts);
    final Set<String> nh = ImmutableSet.copyOf(newHosts);
    final Set<String> prh = ImmutableSet.copyOf(previouslyRemovedHosts);

    // Calculate the freshly removed hosts (current - new) and add in any previously removed hosts
    // that haven't been undeployed yet.
    final Set<String> rh = Sets.union(Sets.difference(ch, nh), prh);

    // Finally, only include hosts that are still registered
    return ImmutableList.copyOf(Sets.intersection(rh, ImmutableSet.copyOf(listHosts())));
  }

  @Override
  public void updateDeploymentGroupHosts(final String groupName, final List<String> hosts)
      throws DeploymentGroupDoesNotExistException {
    log.debug("updating deployment-group hosts: name={}", groupName);
    final ZooKeeperClient client = provider.get("updateDeploymentGroupHosts");
    try {
      final DeploymentGroupStatus status = getDeploymentGroupStatus(groupName);
      if (!allowHostChange(status)) {
        return;
      }

      // statusDeploymentGroupRemovedHosts may not exist for deployment groups created before it was
      // introduced.
      client.ensurePathAndSetData(Paths.statusDeploymentGroupRemovedHosts(groupName),
          Json.asBytesUnchecked(emptyList()));

      final List<String> curHosts = getHosts(client, Paths.statusDeploymentGroupHosts(groupName));
      final List<String> previouslyRemovedHosts = getHosts(
          client, Paths.statusDeploymentGroupRemovedHosts(groupName));

      final List<String> removedHosts = removedHosts(curHosts, hosts, previouslyRemovedHosts);

      if (hosts.equals(curHosts) && removedHosts.equals(previouslyRemovedHosts)) {
        return;
      }

      log.info("for deployment-group name={}, curHosts={}, new hosts={}, "
               + "previouslyRemovedHosts={}, derived removedHosts={}",
          groupName, curHosts, hosts, previouslyRemovedHosts, removedHosts);

      final List<ZooKeeperOperation> ops = Lists.newArrayList();
      ops.add(set(Paths.statusDeploymentGroupHosts(groupName), Json.asBytes(hosts)));
      ops.add(set(Paths.statusDeploymentGroupRemovedHosts(groupName), Json.asBytes(removedHosts)));

      final Node dgn = client.getNode(Paths.configDeploymentGroup(groupName));
      final Integer deploymentGroupVersion = dgn.getStat().getVersion();
      DeploymentGroup deploymentGroup = Json.read(dgn.getBytes(), DeploymentGroup.class);

      List<Map<String, Object>> events = ImmutableList.of();

      if (deploymentGroup.getJobId() != null && updateOnHostChange(deploymentGroup, status)) {
        deploymentGroup = deploymentGroup.toBuilder()
            .setRollingUpdateReason(HOSTS_CHANGED)
            .build();

        // Fail transaction if the deployment group has been updated elsewhere.
        ops.add(check(Paths.configDeploymentGroup(groupName), deploymentGroupVersion));

        // NOTE: If the DG was removed this set() cause the transaction to fail, because
        // removing the DG removes this node. It's *important* that there's an operation that
        // causes the transaction to fail if the DG was removed or we'll end up with
        // inconsistent state.
        ops.add(set(Paths.configDeploymentGroup(deploymentGroup.getName()), deploymentGroup));

        final RollingUpdateOp op = getInitRollingUpdateOps(
            deploymentGroup, hosts, removedHosts, client);
        ops.addAll(op.operations());
        events = op.events();
      }

      log.info("starting zookeeper transaction for updateDeploymentGroupHosts on deployment-group: "
               + "name={} jobId={} operations={}",
          groupName, deploymentGroup.getJobId(), ops);

      client.transaction(ops);
      emitEvents(deploymentGroupEventTopic, events);
    } catch (BadVersionException e) {
      // some other master beat us in processing this host update. not exceptional.
      // ideally we would check the path in the exception, but curator doesn't provide a path
      // for exceptions thrown as part of a transaction.
      log.info("zookeeper transaction for updateDeploymentGroupHosts on deployment-group was "
               + "processed by another master: name={}", groupName);
    } catch (NoNodeException e) {
      throw new DeploymentGroupDoesNotExistException(groupName, e);
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

    log.info("preparing to initiate rolling-update on deployment-group: name={}, jobId={}",
        deploymentGroup.getName(), jobId);

    final DeploymentGroup updated = deploymentGroup.toBuilder()
        .setJobId(jobId)
        .setRolloutOptions(options)
        .setRollingUpdateReason(MANUAL)
        .build();

    if (getJob(jobId) == null) {
      throw new JobDoesNotExistException(jobId);
    }

    final List<ZooKeeperOperation> operations = Lists.newArrayList();
    final ZooKeeperClient client = provider.get("rollingUpdate");

    operations.add(set(Paths.configDeploymentGroup(updated.getName()), updated));

    try {
      final RollingUpdateOp op = getInitRollingUpdateOps(updated, client);
      operations.addAll(op.operations());

      log.info("starting zookeeper transaction for rolling-update on "
               + "deployment-group name={} jobId={}. List of operations: {}",
          deploymentGroup.getName(), jobId, operations);

      client.transaction(operations);

      emitEvents(deploymentGroupEventTopic, op.events());
      log.info("initiated rolling-update on deployment-group: name={}, jobId={}",
          deploymentGroup.getName(), jobId);
    } catch (final NoNodeException e) {
      throw new DeploymentGroupDoesNotExistException(deploymentGroup.getName());
    } catch (final KeeperException e) {
      throw new HeliosRuntimeException(
          "rolling-update on deployment-group " + deploymentGroup.getName() + " failed", e);
    }
  }

  private RollingUpdateOp getInitRollingUpdateOps(final DeploymentGroup deploymentGroup,
                                                  final ZooKeeperClient zooKeeperClient)
      throws DeploymentGroupDoesNotExistException, KeeperException {
    final List<String> hosts = getDeploymentGroupHosts(deploymentGroup.getName());
    return getInitRollingUpdateOps(deploymentGroup, hosts, ImmutableList.of(), zooKeeperClient);
  }

  private RollingUpdateOp getInitRollingUpdateOps(final DeploymentGroup deploymentGroup,
                                                  final List<String> updateHosts,
                                                  final List<String> undeployHosts,
                                                  final ZooKeeperClient zooKeeperClient)
      throws KeeperException {
    final List<RolloutTask> rolloutTasks = new ArrayList<>();

    // give precedence to the updateHosts list so we don't end up in a state where we updated a host
    // and then removed the job from it (because of buggy logic in the calling method)
    final List<String> updateHostsCopy = new ArrayList<>(updateHosts);
    final List<String> undeployHostsCopy = new ArrayList<>(undeployHosts);
    undeployHostsCopy.removeAll(updateHostsCopy);

    // we only care about hosts that are UP
    final List<String> upHostsToUndeploy = undeployHostsCopy.stream()
        .filter(host -> checkHostUp(zooKeeperClient, host))
        .collect(Collectors.toList());
    final List<String> upHostsToDeploy = updateHostsCopy.stream()
        .filter(host -> checkHostUp(zooKeeperClient, host))
        .collect(Collectors.toList());

    rolloutTasks.addAll(RollingUndeployPlanner.of(deploymentGroup)
        .plan(upHostsToUndeploy));
    rolloutTasks.addAll(RollingUpdatePlanner.of(deploymentGroup)
        .plan(upHostsToDeploy));

    log.info("generated rolloutTasks for deployment-group name={} "
             + "updateHosts={} undeployHosts={}: {}",
        deploymentGroup.getName(), updateHosts, undeployHosts, rolloutTasks);

    final DeploymentGroupTasks tasks = DeploymentGroupTasks.newBuilder()
        .setRolloutTasks(rolloutTasks)
        .setTaskIndex(0)
        .setDeploymentGroup(deploymentGroup)
        .build();

    return new RollingUpdateOpFactory(tasks, DEPLOYMENT_GROUP_EVENT_FACTORY)
        .start(deploymentGroup, zooKeeperClient);
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
      case FORCE_UNDEPLOY_JOBS:
        return forceRollingUpdateUndeploy(client, opFactory, deploymentGroup, host);
      case AWAIT_UNDEPLOYED:
        return rollingUpdateAwaitUndeployed(client, opFactory, deploymentGroup, host);
      case MARK_UNDEPLOYED:
        return rollingUpdateMarkUndeployed(client, opFactory, deploymentGroup, host);
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

    for (final Map.Entry<String, VersionedValue<DeploymentGroupTasks>> entry :
        tasksMap.entrySet()) {
      final String deploymentGroupName = entry.getKey();
      final VersionedValue<DeploymentGroupTasks> versionedTasks = entry.getValue();
      final DeploymentGroupTasks tasks = versionedTasks.value();
      final int taskIndex = tasks.getTaskIndex();

      log.info("rolling-update step on deployment-group {}. Doing taskIndex {} of {}: {}. ",
          deploymentGroupName, taskIndex, tasks.getRolloutTasks().size(),
          tasks.getRolloutTasks().get(taskIndex));

      try {
        final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
            tasks, DEPLOYMENT_GROUP_EVENT_FACTORY);
        final RolloutTask task = tasks.getRolloutTasks().get(taskIndex);
        final RollingUpdateOp op = processRollingUpdateTask(
            client, opFactory, task, tasks.getDeploymentGroup());

        if (!op.operations().isEmpty()) {
          final List<ZooKeeperOperation> ops = Lists.newArrayList();
          ops.add(check(Paths.statusDeploymentGroupTasks(deploymentGroupName),
              versionedTasks.version()));
          ops.addAll(op.operations());

          log.info("rolling-update step on deployment-group: name={}, zookeeper operations={}",
              deploymentGroupName, ops);

          try {
            client.transaction(ops);
            emitEvents(deploymentGroupEventTopic, op.events());
          } catch (BadVersionException e) {
            // some other master beat us in processing this rolling update step. not exceptional.
            // ideally we would check the path in the exception, but curator doesn't provide a path
            // for exceptions thrown as part of a transaction.
            log.info("rolling-update step on deployment-group was processed by another master"
                     + ": name={}, zookeeper operations={}", deploymentGroupName, ops);
          } catch (KeeperException e) {
            log.error("rolling-update on deployment-group {} failed", deploymentGroupName, e);
          }
        }
      } catch (final Exception e) {
        log.error("error processing rolling update step for {}", deploymentGroupName, e);
      }
    }
  }

  private void emitEvents(final String topic, final List<Map<String, Object>> events) {
    // Emit events
    for (final Map<String, Object> event : events) {
      final byte[] message = Json.asBytesUnchecked(event);
      for (final EventSender sender : eventSenders) {
        sender.send(topic, message);
      }
    }
  }

  private RollingUpdateOp rollingUpdateTimedoutError(final RollingUpdateOpFactory opFactory,
                                                     final String host,
                                                     final JobId jobId,
                                                     final TaskStatus taskStatus) {
    final List<TaskStatus.State> previousJobStates = getPreviousJobStates(jobId, host, 10);
    final String baseError = "timed out waiting for job " + jobId + " to reach state RUNNING ";
    final String stateInfo = String.format(
        "(terminal job state %s, previous states: %s)",
        taskStatus.getState(),
        Joiner.on("->").join(previousJobStates));

    final Map<String, Object> metadata = Maps.newHashMap();
    metadata.put("jobState", taskStatus.getState());
    metadata.put("previousJobStates", previousJobStates);
    metadata.put("throttleState", taskStatus.getThrottled());

    if (taskStatus.getThrottled().equals(ThrottleState.IMAGE_MISSING)) {
      return opFactory.error(
          baseError + "due to missing Docker image " + stateInfo,
          host,
          RollingUpdateError.IMAGE_MISSING,
          metadata);
    }
    if (taskStatus.getThrottled().equals(ThrottleState.IMAGE_PULL_FAILED)) {
      return opFactory.error(
          baseError + "due to failure pulling Docker image " + stateInfo,
          host,
          RollingUpdateError.IMAGE_PULL_FAILED,
          metadata);
    }
    if (!Strings.isNullOrEmpty(taskStatus.getContainerError())) {
      return opFactory.error(
          baseError + stateInfo + " container error: " + taskStatus.getContainerError(),
          host,
          RollingUpdateError.TIMED_OUT_WAITING_FOR_JOB_TO_REACH_RUNNING,
          metadata);
    }
    return opFactory.error(
        baseError + stateInfo,
        host,
        RollingUpdateError.TIMED_OUT_WAITING_FOR_JOB_TO_REACH_RUNNING,
        metadata);
  }

  private RollingUpdateOp rollingUpdateAwaitRunning(final ZooKeeperClient client,
                                                    final RollingUpdateOpFactory opFactory,
                                                    final DeploymentGroup deploymentGroup,
                                                    final String host) {
    final TaskStatus taskStatus = getTaskStatus(client, host, deploymentGroup.getJobId());
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
        return rollingUpdateTimedoutError(opFactory, host, jobId, taskStatus);
      }

      return opFactory.yield();
    } else {
      // the job is running on the host. last thing we have to ensure is that it was
      // deployed by this deployment group. otherwise some weird conflict has occurred and we
      // won't be able to undeploy the job on the next update.
      final Deployment deployment = getDeployment(host, deploymentGroup.getJobId());
      if (deployment == null) {
        return opFactory.error(
            "deployment for this job not found in zookeeper. "
            + "Perhaps it was manually undeployed?", host,
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
      final String token = MoreObjects.firstNonNull(
          deploymentGroup.getRolloutOptions().getToken(), Job.EMPTY_TOKEN);
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

  private RollingUpdateOp rollingUpdateAwaitUndeployed(final ZooKeeperClient client,
                                                       final RollingUpdateOpFactory opFactory,
                                                       final DeploymentGroup deploymentGroup,
                                                       final String host) {
    final TaskStatus taskStatus = getTaskStatus(client, host, deploymentGroup.getJobId());

    if (taskStatus == null) {
      // The task status (i.e. /status/hosts/<host>/job/<job-id>) has been removed, indicating the
      // job has been undeployed.
      return opFactory.nextTask();
    }

    if (isRolloutTimedOut(client, deploymentGroup)) {
      return opFactory.error("timed out while waiting for job undeployment", host,
          RollingUpdateError.TIMED_OUT_WAITING_FOR_JOB_TO_UNDEPLOY);
    }

    return opFactory.yield();
  }

  private RollingUpdateOp rollingUpdateMarkUndeployed(final ZooKeeperClient client,
                                                      final RollingUpdateOpFactory opFactory,
                                                      final DeploymentGroup deploymentGroup,
                                                      final String host) {
    try {
      final Node node = client.getNode(
          Paths.statusDeploymentGroupRemovedHosts(deploymentGroup.getName()));
      final int version = node.getStat().getVersion();
      final List<String> hostsToUndeploy = Json.read(node.getBytes(), STRING_LIST_TYPE);

      if (!hostsToUndeploy.remove(host)) {
        // Something already removed this host. Don't bother trying to update the removed hosts.
        return opFactory.nextTask();
      }
      /*
      The below check() avoids the following race:

      1. Master A reads list of hosts X and Y
      2. Master B reads list of hosts X and Y
      3. Master A removes host X
      4. Master B removes host Y
      5. Master A commits list with Y and without X
      6. Master B commits list with X and without Y

      We would end up thinking we'd successfully removed both hosts, but in fact would have only
      removed host Y.

      The check() will cause this RollingUpdateOp's ZK transaction to fail without incrementing the
      task index, so this RollingUpdateOp will be retried by the next master that gets to it.
      */
      return opFactory.nextTask(ImmutableList.of(
          check(Paths.statusDeploymentGroupRemovedHosts(deploymentGroup.getName()), version),
          set(Paths.statusDeploymentGroupRemovedHosts(deploymentGroup.getName()),
              Json.asBytes(hostsToUndeploy))));
    } catch (KeeperException | IOException e) {
      return opFactory.error("unable to mark host undeployed after removal from deployment group",
          host, RollingUpdateError.UNABLE_TO_MARK_HOST_UNDEPLOYED);
    }
  }

  /**
   * rollingUpdateUndeploy is used to undeploy jobs during a rolling update. It enables the
   * 'skipRedundantUndeploys' flag, which enables the redundantDeployment() check.
   */
  private RollingUpdateOp rollingUpdateUndeploy(final ZooKeeperClient client,
                                                final RollingUpdateOpFactory opFactory,
                                                final DeploymentGroup deploymentGroup,
                                                final String host) {
    return rollingUpdateUndeploy(client, opFactory, deploymentGroup, host, true);
  }

  private RollingUpdateOp rollingUpdateUndeploy(final ZooKeeperClient client,
                                                final RollingUpdateOpFactory opFactory,
                                                final DeploymentGroup deploymentGroup,
                                                final String host,
                                                final boolean skipRedundantUndeploys) {
    final List<ZooKeeperOperation> operations = Lists.newArrayList();

    for (final Deployment deployment : getTasks(client, host).values()) {

      if (!ownedByDeploymentGroup(deployment, deploymentGroup)
          && !isMigration(deployment, deploymentGroup)) {
        continue;
      }

      if (skipRedundantUndeploys && redundantUndeployment(deployment, deploymentGroup)) {
        continue;
      }

      try {
        final String token = MoreObjects.firstNonNull(
            deploymentGroup.getRolloutOptions().getToken(), Job.EMPTY_TOKEN);
        operations.addAll(getUndeployOperations(client, host, deployment.getJobId(), token));
        log.debug("planned undeploy operations for job={}", deployment.getJobId());
      } catch (TokenVerificationException e) {
        return opFactory.error(e, host, RollingUpdateError.TOKEN_VERIFICATION_ERROR);
      } catch (HostNotFoundException e) {
        return opFactory.error(e, host, RollingUpdateError.HOST_NOT_FOUND);
      } catch (JobNotDeployedException e) {
        // probably somebody beat us to the punch of undeploying. that's fine.
      }
    }

    return opFactory.nextTask(operations);
  }


  /**
   * forceRollingUpdateUndeploy is used to undeploy jobs from hosts that have been removed from the
   * deployment group. It disables the 'skipRedundantUndeploys' flag, which disables the
   * redundantDeployment() check.
   */
  private RollingUpdateOp forceRollingUpdateUndeploy(final ZooKeeperClient client,
                                                     final RollingUpdateOpFactory opFactory,
                                                     final DeploymentGroup deploymentGroup,
                                                     final String host) {
    return rollingUpdateUndeploy(client, opFactory, deploymentGroup, host, false);
  }

  private boolean ownedByDeploymentGroup(final Deployment deployment,
                                         final DeploymentGroup deploymentGroup) {
    return Objects.equals(deployment.getDeploymentGroupName(), deploymentGroup.getName());
  }

  private boolean isMigration(final Deployment deployment, final DeploymentGroup deploymentGroup) {
    return (deploymentGroup.getRolloutOptions().getMigrate()
            && deployment.getJobId().equals(deploymentGroup.getJobId()));
  }

  /**
   * redundantDeployment determines whether or not rollingUpdateUndeploy should actually emit
   * RollingUpdateOps to undeploy a job.
   *
   * <p>Jobs are undeployed in two cases:
   *
   * <p>1. During a rolling update
   * 2. When a host leaves a deployment group
   *
   * <p>In case 1. this redundancy check makes sense. The undeployment of a job during a rolling
   * update is always coupled with the deployment of a job. If the 'new' job is the same job
   * that is currently deployed, the undeployment would be redundant so we do not generate
   * deployment operations.
   *
   * <p>In case 2. undeployment can never be redundant. We always want to undeploy the job from
   * hosts that have left the deployment group. Unfortunately in case case undeployments appear to
   * be redundant to the following checks, so they must be skipped.
   */
  private boolean redundantUndeployment(final Deployment deployment,
                                        final DeploymentGroup deploymentGroup) {
    // This deployment was not created by this deployment group.
    if (!Objects.equals(deployment.getDeploymentGroupName(), deploymentGroup.getName())) {
      return false;
    }

    // This deployment is not of the deployment group's job.
    if (!deployment.getJobId().equals(deploymentGroup.getJobId())) {
      return false;
    }

    // This deployment aims to do something other than start.
    if (!Goal.START.equals(deployment.getGoal())) {
      return false;
    }

    // Undeploying this deployment's job would be redundant because the next operation would simply
    // redeploy the same job.
    return true;
  }

  @Override
  public void stopDeploymentGroup(final String deploymentGroupName)
      throws DeploymentGroupDoesNotExistException {
    checkNotNull(deploymentGroupName, "name");

    log.info("stop deployment-group: name={}", deploymentGroupName);

    final ZooKeeperClient client = provider.get("stopDeploymentGroup");

    // Delete deployment group tasks (if any) and set DG state to FAILED
    final DeploymentGroupStatus status = DeploymentGroupStatus.newBuilder()
        .setState(FAILED)
        .setError("Stopped by user")
        .build();
    final String statusPath = Paths.statusDeploymentGroup(deploymentGroupName);
    final String tasksPath = Paths.statusDeploymentGroupTasks(deploymentGroupName);

    try {
      client.ensurePath(Paths.statusDeploymentGroupTasks());

      final List<ZooKeeperOperation> operations = Lists.newArrayList();

      // NOTE: This remove operation is racey. If tasks exist and the rollout finishes before the
      // delete() is executed then this will fail. Conversely, if it doesn't exist but is created
      // before the transaction is executed it will also fail. This is annoying for users, but at
      // least means we won't have inconsistent state.
      //
      // That the set() is first in the list of operations is important because of the
      // kludgy error checking we do below to disambiguate "doesn't exist" failures from the race
      // condition mentioned below.
      operations.add(set(statusPath, status));

      final Stat tasksStat = client.exists(tasksPath);
      if (tasksStat != null) {
        operations.add(delete(tasksPath));
      } else {
        // There doesn't seem to be a "check that node doesn't exist" operation so we
        // do a create and a delete on the same path to emulate it.
        operations.add(create(tasksPath));
        operations.add(delete(tasksPath));
      }

      client.transaction(operations);
    } catch (final NoNodeException e) {
      // Either the statusPath didn't exist, in which case the DG does not exist. Or the tasks path
      // does not exist which can happen due to the race condition described above. In the latter
      // case make sure we don't return a "doesn't exist" error as that would be a lie.
      // Yes, the way you figure out which operation in a transaction failed is retarded.
      if (((OpResult.ErrorResult) e.getResults().get(0)).getErr()
          == KeeperException.Code.NONODE.intValue()) {
        throw new DeploymentGroupDoesNotExistException(deploymentGroupName);
      } else {
        throw new HeliosRuntimeException(
            "stop deployment-group " + deploymentGroupName
            + " failed due to a race condition, please retry", e);
      }
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

  private List<String> getHosts(final ZooKeeperClient client, final String path) {
    try {
      return Json.read(client.getNode(path).getBytes(), STRING_LIST_TYPE);
    } catch (JsonMappingException | JsonParseException | NoNodeException e) {
      return emptyList();
    } catch (KeeperException | IOException e) {
      throw new HeliosRuntimeException("failed to read deployment group hosts from " + path, e);
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

    return getHosts(client, Paths.statusDeploymentGroupHosts(name));
  }

  /**
   * Returns the job configuration for the job specified by {@code id} as a
   * {@link Job} object. A return value of null indicates the job doesn't exist.
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
      throw new RuntimeException(e);
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

    // Delete job history on a best effort basis
    try {
      client.deleteRecursive(Paths.historyJob(id));
    } catch (NoNodeException ignored) {
      // There's no history for this job
    } catch (KeeperException e) {
      log.warn("error removing job history for job {}", id, e);
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
      throw new RuntimeException(e);
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
      throw new HeliosRuntimeException("3 failures (possibly concurrent modifications) while "
                                       + "deploying. Giving up.");
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
      throw new HeliosRuntimeException("updating deployment " + deployment
                                       + " on host " + host + " failed", e);
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
    final ZooKeeperClient client = provider.get("getHostStatus");

    if (!ZooKeeperRegistrarUtil.isHostRegistered(client, host)) {
      log.warn("Host {} isn't registered in ZooKeeper.", host);
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

  @Override
  public Map<String, String> getHostLabels(final String host) {
    final ZooKeeperClient client = provider.get("getHostStatus");

    if (!ZooKeeperRegistrarUtil.isHostRegistered(client, host)) {
      return emptyMap();
    }

    final Map<String, String> labels = getLabels(client, host);
    return labels == null ? emptyMap() : labels;
  }

  @Override
  public boolean isHostUp(final String host) {
    final ZooKeeperClient client = provider.get("isHostUp");
    return ZooKeeperRegistrarUtil.isHostRegistered(client, host) && checkHostUp(client, host);
  }

  @Override
  public AgentInfo getAgentInfo(final String host) {
    return getAgentInfo(provider.get("getAgentInfo"), host);
  }

  private AgentInfo getAgentInfo(final ZooKeeperClient client, final String host) {
    return tryGetEntity(client, Paths.statusHostAgentInfo(host), AGENT_INFO_TYPE, "agent info");
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
    for (final String jobIdString : jobIdStrings) {
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
      throw new HeliosRuntimeException("Getting task " + jobId + " status "
                                       + "for host " + host + " failed", e);
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
        log.warn("Unable to get deployment config for {}", host, e);
        return ImmutableMap.of();
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
      throw new RuntimeException(e);
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
      for (final int port : staticPorts) {
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
      for (final int port : staticPorts) {
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

  private static void checkForPortConflicts(final ZooKeeperClient client,
                                            final String host,
                                            final int port,
                                            final JobId jobId)
      throws JobPortAllocationConflictException {
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
      final List<TaskStatusEvent> cappedJobHistory = jobHistory.subList(
          0, Math.min(maxStates, jobHistory.size()));
      final Function<TaskStatusEvent, TaskStatus.State> statusesToStrings =
          new Function<TaskStatusEvent, TaskStatus.State>() {
            @Override
            public TaskStatus.State apply(@Nullable TaskStatusEvent input) {
              if (input != null) {
                return input.getStatus().getState();
              }
              return null;
            }
          };
      previousStates = Lists.transform(cappedJobHistory, statusesToStrings);
    } catch (JobDoesNotExistException ignored) {
      previousStates = emptyList();
    }

    return previousStates;
  }
}
