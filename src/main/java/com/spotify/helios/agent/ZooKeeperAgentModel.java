/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import com.spotify.helios.common.Json;
import com.spotify.helios.common.coordination.Node;
import com.spotify.helios.common.coordination.Paths;
import com.spotify.helios.common.coordination.PersistentPathChildrenCache;
import com.spotify.helios.common.coordination.ZooKeeperClient;
import com.spotify.helios.common.coordination.ZooKeeperPersistentNodeRemover;
import com.spotify.helios.common.coordination.ZooKeeperUpdatingPersistentMap;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.Task;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.spotify.helios.common.descriptors.Descriptor.parse;
import static com.spotify.helios.common.descriptors.Goal.UNDEPLOY;

public class ZooKeeperAgentModel extends AbstractAgentModel {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperAgentModel.class);

  private static final String TASK_CONFIG_FILENAME = "task-config.json";
  private static final String TASK_STATUS_FILENAME = "task-status.json";
  private static final String TASK_REMOVER_FILENAME = "remove.json";

  private final PersistentPathChildrenCache tasks;
  private final ZooKeeperUpdatingPersistentMap taskStatuses;
  private final ZooKeeperPersistentNodeRemover taskRemover;

  private final ZooKeeperClient client;
  private final String agent;

  public ZooKeeperAgentModel(final ZooKeeperClient client, final String agent,
                             final Path stateDirectory) throws IOException {
    this.client = checkNotNull(client);
    this.agent = checkNotNull(agent);
    final Path taskConfigFile = stateDirectory.resolve(TASK_CONFIG_FILENAME);
    this.tasks = client.pathChildrenCache(Paths.configAgentJobs(agent), taskConfigFile);
    tasks.addListener(new JobsListener());
    final Path taskStatusFile = stateDirectory.resolve(TASK_STATUS_FILENAME);
    this.taskStatuses = ZooKeeperUpdatingPersistentMap.create(client, taskStatusFile);
    final Path removerFile = stateDirectory.resolve(TASK_REMOVER_FILENAME);
    this.taskRemover = ZooKeeperPersistentNodeRemover.create(client, removerFile,
                                                             new TaskRemovalPredicate());
  }

  private JobId jobIdFromTaskPath(final String path) {
    final String prefix = Paths.configAgentJobs(agent) + "/";
    return JobId.fromString(path.replaceFirst(prefix, ""));
  }

  private JobId jobIdFromTaskStatusPath(final String path) {
    final String prefix = Paths.statusAgentJobs(agent) + "/";
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
    for (Map.Entry<String, byte[]> entry : this.taskStatuses.entrySet()) {
      try {
        final JobId id = jobIdFromTaskStatusPath(entry.getKey());
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
    taskStatuses.put(Paths.statusAgentJob(agent, jobId), status.toJsonBytes());

    // TODO (dano): restore task status history and make sure that it's bounded as well
//    final String historyPath = Paths.historyJobAgentEventsTimestamp(
//        jobId, agent, System.currentTimeMillis());
//    client.createAndSetData(historyPath, status.toJsonBytes());
  }

  @Override
  public TaskStatus getTaskStatus(final JobId jobId) {
    final String path = Paths.statusAgentJob(agent, jobId);
    final byte[] data = taskStatuses.get(path);
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
    final String path = Paths.statusAgentJob(agent, jobId);
    taskStatuses.remove(path);
  }

  @Override
  public void close() throws InterruptedException {
    tasks.close();
    taskStatuses.close();
    taskRemover.close();
  }

  @Override
  public void removeUndeployTombstone(final JobId jobId) {
    String path = Paths.configAgentJob(agent, jobId);
    taskRemover.remove(path);
  }

  public void start() {
    log.debug("starting");
    try {
      tasks.start();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private class JobsListener implements PersistentPathChildrenCache.Listener {

    @Override
    public void nodesChanged(final PersistentPathChildrenCache cache) {
      fireTasksUpdated();
    }
  }

  private class TaskRemovalPredicate implements Predicate<Node> {

    @Override
    public boolean apply(final Node node) {
      try {
        final Task task = parse(node.getBytes(), Task.class);
        return task.getGoal() == UNDEPLOY;
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
