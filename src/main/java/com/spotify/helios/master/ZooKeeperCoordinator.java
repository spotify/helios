/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.master;

import com.google.common.collect.Maps;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.coordination.*;
import com.spotify.helios.common.descriptors.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.spotify.helios.common.descriptors.Descriptor.parse;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class ZooKeeperCoordinator implements Coordinator {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperCoordinator.class);

  public static final Map<String, JobStatus> EMPTY_STATUSES = emptyMap();

  private final CuratorInterface client;

  public ZooKeeperCoordinator(final CuratorInterface client) {
    this.client = client;
  }

  @Override
  public void addAgent(final String agent) throws HeliosException {
    log.debug("adding agent: {}", agent);

    try {
      client.ensurePath(Paths.configAgent(agent));
      client.ensurePath(Paths.configAgentJobs(agent));
    } catch (Exception e) {
      throw new HeliosException("adding slave " + agent + " failed", e);
    }
  }

  @Override
  public List<String> getAgents() throws HeliosException {
    try {
      return client.getChildren(Paths.configAgents());
    } catch (KeeperException.NoNodeException e) {
      return emptyList();
    } catch (KeeperException e) {
      throw new HeliosException("listing agents failed", e);
    }
  }

  @Override
  public void removeAgent(final String agent) throws HeliosException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addJob(final JobDescriptor job) throws HeliosException {
    log.debug("adding job: {}", job);
    try {
      final String path = Paths.configJob(job.getId());
      client.createAndSetData(path, job.toJsonBytes());
    } catch (KeeperException.NodeExistsException e) {
      throw new JobExistsException(job.getId());
    } catch (KeeperException e) {
      throw new HeliosException("adding job " + job + " failed", e);
    }
  }

  @Override
  public JobDescriptor getJob(final String id) throws HeliosException {
    log.debug("getting job: {}", id);
    final String path = Paths.configJob(id);
    try {
      final byte[] data = client.getData(path);
      return Descriptor.parse(data, JobDescriptor.class);
    } catch (NoNodeException e) {
      throw new JobDoesNotExistException(e);
    } catch (KeeperException | IOException e) {
      throw new HeliosException("getting job " + id + " failed", e);
    }
  }

  @Override
  public Map<String, JobDescriptor> getJobs() throws HeliosException {
    log.debug("getting jobs");
    final String folder = Paths.configJobs();
    try {
      final List<String> ids = client.getChildren(folder);
      final Map<String, JobDescriptor> descriptors = Maps.newHashMap();
      for (final String id : ids) {
        final String path = Paths.configJobPath(id);
        final byte[] data = client.getData(path);
        final JobDescriptor descriptor = parse(data, JobDescriptor.class);
        descriptors.put(descriptor.getId(), descriptor);
      }
      return descriptors;
    } catch (KeeperException | IOException e) {
      throw new HeliosException("getting jobs failed", e);
    }
  }

  @Override
  public JobDescriptor removeJob(final String id) throws HeliosException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addAgentJob(final String agent, final AgentJob agentJob)
      throws HeliosException {
    log.debug("adding agent job: agent={}, job={}", agent, agentJob);

    final String job = agentJob.getJob();
    final JobDescriptor descriptor = getJob(job);

    if (descriptor == null) {
      throw new JobDoesNotExistException(job);
    }
    // TODO(drewc): do the assert and the createAndSetData in a txn
    assertAgentExists(agent);
    final String path = Paths.configAgentJob(agent, job);
    final AgentJobDescriptor agentJobDescriptor = new AgentJobDescriptor(agentJob, descriptor);
    try {
      client.createAndSetData(path, agentJobDescriptor.toJsonBytes());
    } catch (NodeExistsException e) {
      throw new JobAlreadyDeployedException(agent, job);
    } catch (Exception e) {
      throw new HeliosException("adding slave container failed", e);
    }
  }

  private void assertAgentExists(String agent) throws HeliosException {
    try {
      client.getData(Paths.statusAgent(agent));
    } catch (NoNodeException e) {
      throw new AgentDoesNotExistException(agent, e);
    } catch (KeeperException e) {
      throw new HeliosException(e);
    }
  }

  @Override
  public AgentJob getAgentJob(final String agent, final String jobId)
      throws HeliosException {
    final String path = Paths.configAgentJob(agent, jobId);
    try {
      final byte[] data = client.getData(path);
      final AgentJobDescriptor descriptor = parse(data, AgentJobDescriptor.class);
      return descriptor.getJob();
    } catch (KeeperException.NoNodeException e) {
      return null;
    } catch (KeeperException | IOException e) {
      throw new HeliosException("getting slave container failed", e);
    }
  }

  @Override
  public AgentStatus getAgentStatus(final String agent)
      throws HeliosException {
    final Map<String, AgentJob> jobs = getAgentJobs(agent);
    final Map<String, JobStatus> statuses = getAgentStatuses(agent);
    if (jobs == null) {
      return null;
    }
    return new AgentStatus(jobs, statuses == null ? EMPTY_STATUSES : statuses);
  }

  private Map<String, JobStatus> getAgentStatuses(final String agent) throws HeliosException {
    // Job status
    final Map<String, JobStatus> statuses = Maps.newHashMap();
    try {
      final List<String> jobIds;
      final String folder = Paths.statusAgentJobs(agent);
      try {
        jobIds = client.getChildren(folder);
      } catch (KeeperException.NoNodeException e) {
        return null;
      }

      for (final String jobId : jobIds) {
        final String containerPath = Paths.statusAgentJob(agent, jobId);
        try {
          final byte[] data = client.getData(containerPath);
          statuses.put(jobId, parse(data, JobStatus.class));
        } catch (KeeperException.NoNodeException ignored) {
          log.debug("agent job status node disappeared: {}", jobId);
        }
      }
    } catch (KeeperException | IOException e) {
      throw new HeliosException("getting agent job status failed", e);
    }

    return statuses;
  }

  private Map<String, AgentJob> getAgentJobs(final String agent) throws HeliosException {
    final Map<String, AgentJob> jobs = Maps.newHashMap();
    try {
      final String folder = Paths.configAgentJobs(agent);
      final List<String> jobIds;
      try {
        jobIds = client.getChildren(folder);
      } catch (KeeperException.NoNodeException e) {
        return null;
      }

      for (final String jobId : jobIds) {
        final String containerPath = Paths.configAgentJob(agent, jobId);
        try {
          final byte[] data = client.getData(containerPath);
          final AgentJobDescriptor agentJobDescriptor = parse(data, AgentJobDescriptor.class);
          jobs.put(jobId, agentJobDescriptor.getJob());
        } catch (KeeperException.NoNodeException ignored) {
          log.debug("agent job config node disappeared: {}", jobId);
        }
      }
    } catch (KeeperException | IOException e) {
      throw new HeliosException("getting agent job config failed", e);
    }

    return jobs;
  }

  @Override
  public AgentJob removeAgentJob(final String agent, final String jobId)
      throws HeliosException {
    log.debug("removing agent job: agent={}, job={}", agent, jobId);

    final AgentJob job = getAgentJob(agent, jobId);
    if (job == null) {
      return null;
    }

    final String path = Paths.configAgentJob(agent, jobId);

    try {
      client.delete(path);
    } catch (KeeperException.NoNodeException ignore) {
    } catch (KeeperException e) {
      throw new HeliosException("removing agent job failed", e);
    }

    return job;
  }
}
