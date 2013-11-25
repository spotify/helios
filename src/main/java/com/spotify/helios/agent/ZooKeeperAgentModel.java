/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.spotify.helios.common.coordination.ZooKeeperClient;
import com.spotify.helios.common.coordination.Paths;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.Task;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.netflix.curator.framework.recipes.cache.PathChildrenCache.StartMode.POST_INITIALIZED_EVENT;
import static com.spotify.helios.common.descriptors.Descriptor.parse;
import static org.apache.zookeeper.KeeperException.NoNodeException;

public class ZooKeeperAgentModel extends AbstractAgentModel {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperAgentModel.class);

  private final PathChildrenCache jobs;
  private final CountDownLatch jobsInitialized = new CountDownLatch(1);

  private final ZooKeeperClient client;
  private final String agent;

  public ZooKeeperAgentModel(final ZooKeeperClient client, final String agent) {
    this.client = checkNotNull(client);
    this.agent = checkNotNull(agent);
    this.jobs = client.pathChildrenCache(Paths.configAgentJobs(agent), true);
    jobs.getListenable().addListener(new ContainersListener());
  }

  private JobId jobId(final String path) {
    final String prefix = Paths.configAgentJobs(agent) + "/";
    return JobId.fromString(path.replaceFirst(prefix, ""));
  }

  @Override
  public Map<JobId, TaskStatus> getTaskStatuses() {
    final String path = Paths.statusAgentJobs(agent);
    final Map<JobId, TaskStatus> jobs = Maps.newHashMap();
    try {
      final List<String> children = client.getChildren(path);
      for (final String jobIdString : children) {
        final JobId jobId = JobId.fromString(jobIdString);
        final TaskStatus taskStatus = getTaskStatus(jobId);
        jobs.put(jobId, taskStatus);
      }
    } catch (KeeperException e) {
      throw Throwables.propagate(e);
    }
    return jobs;
  }

  @Override
  public void setTaskStatus(final JobId jobId, final TaskStatus status) {
    log.debug("setting job status: {}", status);

    final String path = Paths.statusAgentJob(agent, jobId);

    try {
      // Check if the node already exists.
      final Stat stat = client.stat(path);

      byte[] jsonBytes = status.toJsonBytes();
      if (stat != null) {
        // The node already exists, overwrite it.
        client.setData(path, jsonBytes);
      } else {
        client.createAndSetData(path, jsonBytes);
      }
      String historyPath = Paths.historyJobAgent(jobId.toString(), agent, new Instant().getMillis());
      client.createAndSetData(historyPath, jsonBytes);
    } catch (KeeperException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public TaskStatus getTaskStatus(final JobId jobId) {
    final String path = Paths.statusAgentJob(agent, jobId);
    try {
      final byte[] data = client.getData(path);
      if (data == null) {
        // No data, treat that as no state
        return null;
      }
      return parse(data, TaskStatus.class);
    } catch (NoNodeException e) {
      // No node -> no state
      return null;
    } catch (IOException e) {
      // State couldn't be parsed, treat that as no state
      return null;
    } catch (KeeperException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void removeTaskStatus(final JobId jobId) {
    log.debug("removing job status: name={}", jobId);
    try {
      client.delete(Paths.statusAgentJob(agent, jobId));
    } catch (NoNodeException e) {
      log.debug("application node did not exist");
    } catch (KeeperException e) {
      throw Throwables.propagate(e);
    }
  }

  public void start() {
    log.debug("starting");
    try {
      jobs.start(POST_INITIALIZED_EVENT);
      jobsInitialized.await();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private class ContainersListener implements PathChildrenCacheListener {

    @Override
    public void childEvent(final CuratorFramework client, final PathChildrenCacheEvent event)
        throws Exception {
      log.debug("agent jobs event: event={}", event);

      switch (event.getType()) {
        case CHILD_ADDED: {
          final byte[] data = event.getData().getData();
          final JobId jobId = jobId(event.getData().getPath());
          final Task descriptor = parse(data, Task.class);
          doAddJob(jobId, descriptor);
          break;
        }
        case CHILD_UPDATED: {
          final byte[] data = event.getData().getData();
          final JobId jobId = jobId(event.getData().getPath());
          final Task descriptor = parse(data, Task.class);
          doUpdateJob(jobId, descriptor);
          break;
        }
        case CHILD_REMOVED: {
          final JobId jobId = jobId(event.getData().getPath());
          doRemoveJob(jobId);
          break;
        }
        case INITIALIZED:
          jobsInitialized.countDown();
          break;
      }
    }
  }
}
