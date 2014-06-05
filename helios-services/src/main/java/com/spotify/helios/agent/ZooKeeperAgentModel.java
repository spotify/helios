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

public class ZooKeeperAgentModel extends AbstractIdleService implements AgentModel {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperAgentModel.class);

  private static final String TASK_CONFIG_FILENAME = "task-config.json";
  private static final String TASK_HISTORY_FILENAME = "task-history.json";
  private static final String TASK_STATUS_FILENAME = "task-status.json";
  private static final String TASK_REMOVER_FILENAME = "remove.json";
  private static final Predicate<Node> TASK_GOAL_IS_UNDEPLOY = new TaskGoalIsUndeployPredicate();

  private final PersistentPathChildrenCache tasks;
  private final ZooKeeperUpdatingPersistentDirectory taskStatuses;
  private final ZooKeeperPersistentNodeRemover taskRemover;
  private final QueueingHistoryWriter historyWriter;

  private final String agent;
  private final CopyOnWriteArrayList<AgentModel.Listener> listeners = new CopyOnWriteArrayList<>();

  public ZooKeeperAgentModel(final ZooKeeperClientProvider provider, final String host,
                             final Path stateDirectory) throws IOException {
    // TODO(drewc): we're constructing too many heavyweight things in the ctor, these kinds of
    // things should be passed in/provider'd/etc.
    final ZooKeeperClient client = provider.get("ZooKeeperAgentModel_ctor");
    this.agent = checkNotNull(host);
    final Path taskConfigFile = stateDirectory.resolve(TASK_CONFIG_FILENAME);
    this.tasks = client.pathChildrenCache(Paths.configHostJobs(host), taskConfigFile);
    tasks.addListener(new JobsListener());
    final Path taskStatusFile = stateDirectory.resolve(TASK_STATUS_FILENAME);

    this.taskStatuses = ZooKeeperUpdatingPersistentDirectory.create("agent-model-task-statuses",
                                                                    provider,
                                                                    taskStatusFile,
                                                                    Paths.statusHostJobs(host));
    final Path removerFile = stateDirectory.resolve(TASK_REMOVER_FILENAME);
    this.taskRemover = ZooKeeperPersistentNodeRemover.create("agent-model-task-remover", provider,
                                                             removerFile, TASK_GOAL_IS_UNDEPLOY,
                                                             true);
    this.historyWriter = new QueueingHistoryWriter(host, client,
        stateDirectory.resolve(TASK_HISTORY_FILENAME));
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
    final String prefix = Paths.configHostJobs(agent) + "/";
    return JobId.fromString(path.replaceFirst(prefix, ""));
  }

  @Override
  public Map<JobId, Task> getTasks() {
    final Map<JobId, Task> tasks = Maps.newHashMap();
    for (Map.Entry<String, byte[]> entry : this.tasks.getNodes().entrySet()) {
      try {
        final JobId id = jobIdFromTaskPath(entry.getKey());
        final Task task = Json.read(entry.getValue(), Task.class);
        tasks.put(id, task);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    return tasks;
  }

  @Override
  public Map<JobId, TaskStatus> getTaskStatuses() {
    final Map<JobId, TaskStatus> statuses = Maps.newHashMap();
    for (Map.Entry<String, byte[]> entry : this.taskStatuses.map().entrySet()) {
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

  @Override
  public void setTaskStatus(final JobId jobId, final TaskStatus status) {
    log.debug("setting task status: {}", status);
    taskStatuses.map().put(jobId.toString(), status.toJsonBytes());
    historyWriter.saveHistoryItem(jobId, status);
  }

  @Override
  public TaskStatus getTaskStatus(final JobId jobId) {
    final byte[] data = taskStatuses.map().get(jobId.toString());
    if (data == null) {
      return null;
    }
    try {
      return parse(data, TaskStatus.class);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void removeTaskStatus(final JobId jobId) {
    taskStatuses.map().remove(jobId.toString());
  }

  @Override
  public void removeUndeployTombstone(final JobId jobId) {
    String path = Paths.configHostJob(agent, jobId);
    taskRemover.remove(path);
  }

  @Override
  public void addListener(final AgentModel.Listener listener) {
    listeners.add(listener);
    listener.tasksChanged(this);
  }

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
    public void nodesChanged(final PersistentPathChildrenCache cache) {
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
