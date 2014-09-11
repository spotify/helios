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

package com.spotify.helios.agent;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;

import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.Task;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.servicescommon.coordination.Node;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.PersistentPathChildrenCache;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClientProvider;
import com.spotify.helios.servicescommon.coordination.ZooKeeperPersistentNodeRemover;
import com.spotify.helios.servicescommon.coordination.ZooKeeperUpdatingPersistentDirectory;

import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.spotify.helios.common.descriptors.Descriptor.parse;
import static com.spotify.helios.common.descriptors.Goal.UNDEPLOY;

/**
 * The Helios Agent's view into ZooKeeper.
 *
 * This caches ZK state to local disk so the agent can continue to function in the face of a ZK
 * outage.
 */
public class ZooKeeperAgentModel extends AbstractIdleService implements AgentModel {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperAgentModel.class);

  private static final String TASK_CONFIG_FILENAME = "task-config.json";
  private static final String TASK_HISTORY_FILENAME = "task-history.json";
  private static final String TASK_STATUS_FILENAME = "task-status.json";
  private static final String TASK_REMOVER_FILENAME = "remove.json";
  private static final Predicate<Node> TASK_GOAL_IS_UNDEPLOY = new TaskGoalIsUndeployPredicate();

  private final PersistentPathChildrenCache<Task> tasks;
  private final ZooKeeperUpdatingPersistentDirectory taskStatuses;
  private final ZooKeeperPersistentNodeRemover taskRemover;
  private final QueueingHistoryWriter historyWriter;
  private final Paths paths;

  private final String agent;
  private final CopyOnWriteArrayList<AgentModel.Listener> listeners = new CopyOnWriteArrayList<>();


  public ZooKeeperAgentModel(final ZooKeeperClientProvider provider, final String host,
                             final Path stateDirectory, final Paths paths)
                                 throws IOException, InterruptedException {
    this.paths = paths;
    // TODO(drewc): we're constructing too many heavyweight things in the ctor, these kinds of
    // things should be passed in/provider'd/etc.
    final ZooKeeperClient client = provider.get("ZooKeeperAgentModel_ctor");
    this.agent = checkNotNull(host);
    final Path taskConfigFile = stateDirectory.resolve(TASK_CONFIG_FILENAME);

    this.tasks = client.pathChildrenCache(paths.configHostJobs(host), taskConfigFile,
                                          Json.type(Task.class));
    tasks.addListener(new JobsListener());
    final Path taskStatusFile = stateDirectory.resolve(TASK_STATUS_FILENAME);

    this.taskStatuses = ZooKeeperUpdatingPersistentDirectory.create("agent-model-task-statuses",
                                                                    provider,
                                                                    taskStatusFile,
                                                                    paths.statusHostJobs(host));
    final Path removerFile = stateDirectory.resolve(TASK_REMOVER_FILENAME);
    this.taskRemover = ZooKeeperPersistentNodeRemover.create("agent-model-task-remover", provider,
                                                             removerFile, TASK_GOAL_IS_UNDEPLOY,
                                                             true);
    this.historyWriter = new QueueingHistoryWriter(host, client,
        stateDirectory.resolve(TASK_HISTORY_FILENAME), paths);
  }

  @Override
  protected void startUp() throws Exception {
    tasks.startAsync().awaitRunning();
    taskStatuses.startAsync().awaitRunning();
    taskRemover.startAsync().awaitRunning();
    historyWriter.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    tasks.stopAsync().awaitTerminated();
    taskStatuses.stopAsync().awaitTerminated();
    taskRemover.stopAsync().awaitTerminated();
    historyWriter.stopAsync().awaitTerminated();
  }

  private JobId jobIdFromTaskPath(final String path) {
    final String prefix = paths.configHostJobs(agent) + "/";
    return JobId.fromString(path.replaceFirst(prefix, ""));
  }

  /**
   * Returns the tasks (basically, a pair of {@link Job} and {@Goal}) for the current agent.
   */
  @Override
  public Map<JobId, Task> getTasks() {
    final Map<JobId, Task> tasks = Maps.newHashMap();
    for (Map.Entry<String, Task> entry : this.tasks.getNodes().entrySet()) {
      final JobId id = jobIdFromTaskPath(entry.getKey());
      tasks.put(id, entry.getValue());
    }
    return tasks;
  }

  /**
   * Returns the {@link TaskStatus}es for all tasks assigned to the current agent.
   */
  @Override
  public Map<JobId, TaskStatus> getTaskStatuses() {
    final Map<JobId, TaskStatus> statuses = Maps.newHashMap();
    for (Map.Entry<String, byte[]> entry : this.taskStatuses.entrySet()) {
      try {
        final JobId id = JobId.fromString(entry.getKey());
        final TaskStatus status = Json.read(entry.getValue(), TaskStatus.class);
        statuses.put(id, status);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    return statuses;
  }

  /**
   * Set the {@link TaskStatus} for the job identified by {@code jobId}.
   */
  @Override
  public void setTaskStatus(final JobId jobId, final TaskStatus status)
      throws InterruptedException {
    log.debug("setting task status: {}", status);
    taskStatuses.put(jobId.toString(), status.toJsonBytes());
    historyWriter.saveHistoryItem(jobId, status);
  }

  /**
   * Get the {@link TaskStatus} for the job identified by {@code jobId}.
   */
  @Override
  public TaskStatus getTaskStatus(final JobId jobId) {
    final byte[] data = taskStatuses.get(jobId.toString());
    if (data == null) {
      return null;
    }
    try {
      return parse(data, TaskStatus.class);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Remove the {@link TaskStatus} for the job identified by {@code jobId}.
   */
  @Override
  public void removeTaskStatus(final JobId jobId) throws InterruptedException {
    taskStatuses.remove(jobId.toString());
  }

  /**
   * Remove the tombstone for the job identified by {@code jobId}.  Tombstones are written by the
   * master to tell the agent to undeploy, and these tombstones are removed by the agent after it
   * has undeployed.
   */
  @Override
  public void removeUndeployTombstone(final JobId jobId) throws InterruptedException {
    String path = paths.configHostJob(agent, jobId);
    taskRemover.remove(path);
  }

  /**
   * Add a listener that will be notified when tasks are changed.
   */
  @Override
  public void addListener(final AgentModel.Listener listener) {
    listeners.add(listener);
    listener.tasksChanged(this);
  }

  /**
   * Remove a listener that will be notified when tasks are changed.
   */
  @Override
  public void removeListener(final AgentModel.Listener listener) {
    listeners.remove(listener);
  }

  protected void fireTasksUpdated() {
    for (final AgentModel.Listener listener : listeners) {
      try {
        listener.tasksChanged(this);
      } catch (Exception e) {
        log.error("listener threw exception", e);
      }
    }
  }

  private class JobsListener implements PersistentPathChildrenCache.Listener {

    @Override
    public void nodesChanged(final PersistentPathChildrenCache<?> cache) {
      fireTasksUpdated();
    }

    @Override
    public void connectionStateChanged(final ConnectionState state) {
      // ignore
    }
  }

  private static class TaskGoalIsUndeployPredicate implements Predicate<Node> {

    @Override
    public boolean apply(final Node node) {
      assert node != null;
      try {
        final Task task = parse(node.getBytes(), Task.class);
        return task.getGoal() == UNDEPLOY;
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
