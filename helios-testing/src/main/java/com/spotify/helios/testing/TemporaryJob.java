/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.fail;

public class TemporaryJob {

  private static final Logger log = LoggerFactory.getLogger(TemporaryJob.class);

  private static final long TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5);

  private final Map<String, TaskStatus> statuses = Maps.newHashMap();
  private final HeliosClient client;
  private final Prober prober;
  private final Job job;
  private final List<String> hosts;
  private final Set<String> waitPorts;

  TemporaryJob(final HeliosClient client, final Prober prober, final Job job,
               final List<String> hosts, final Set<String> waitPorts) {
    this.client = checkNotNull(client, "client");
    this.prober = checkNotNull(prober, "prober");
    this.job = checkNotNull(job, "job");
    this.hosts = ImmutableList.copyOf(checkNotNull(hosts, "hosts"));
    this.waitPorts = ImmutableSet.copyOf(checkNotNull(waitPorts, "waitPorts"));
  }

  public Job job() {
    return job;
  }

  public Integer port(final String host, final String port) {
    checkArgument(hosts.contains(host), "host %s not found", host);
    checkArgument(job.getPorts().containsKey(port), "port %s not found", port);
    final TaskStatus status = statuses.get(host);
    if (status == null) {
      return null;
    }
    final PortMapping portMapping = status.getPorts().get(port);
    if (portMapping == null) {
      return null;
    }
    return portMapping.getExternalPort();
  }

  public List<HostAndPort> addresses(final String port) {
    checkArgument(job.getPorts().containsKey(port), "port %s not found", port);
    final List<HostAndPort> addresses = Lists.newArrayList();
    for (Map.Entry<String, TaskStatus> entry : statuses.entrySet()) {
      final Integer externalPort = entry.getValue().getPorts().get(port).getExternalPort();
      assert externalPort != null;
      addresses.add(HostAndPort.fromParts(entry.getKey(), externalPort));
    }
    return addresses;
  }

  void deploy() {
    try {
      // Create job
      final CreateJobResponse createResponse = get(client.createJob(job));
      if (createResponse.getStatus() != CreateJobResponse.Status.OK) {
        fail(format("Failed to create job %s - %s", job.getId(),
                    createResponse.toString()));
      }

      // Deploy job
      final Deployment deployment = Deployment.of(job.getId(), Goal.START);
      for (final String host : hosts) {
        final JobDeployResponse deployResponse = get(client.deploy(deployment, host));
        if (deployResponse.getStatus() != JobDeployResponse.Status.OK) {
          fail(format("Failed to deploy job %s %s - %s",
                      job.getId(), job.toString(), deployResponse));
        }
      }

      // Wait for job to come up
      for (final String host : hosts) {
        awaitUp(host);
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      fail(format("Failed to deploy job %s %s - %s",
                  job.getId(), job.toString(), e));
    }
  }

  void undeploy(final List<AssertionError> errors) {
    for (String host : hosts) {
      final JobId jobId = job.getId();
      final JobUndeployResponse response;
      try {
        response = get(client.undeploy(jobId, host));
        if (response.getStatus() != JobUndeployResponse.Status.OK &&
            response.getStatus() != JobUndeployResponse.Status.JOB_NOT_FOUND) {
          errors.add(new AssertionError(format("Failed to undeploy job %s - %s",
                                               job.getId(), response)));
        }
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        errors.add(new AssertionError(e));
      }
    }

    try {
      final JobDeleteResponse response = get(client.deleteJob(job.getId()));
      if (response.getStatus() != JobDeleteResponse.Status.OK) {
        errors.add(new AssertionError(format("Failed to delete job %s - %s",
                                             job.getId().toString(), response.toString())));
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      errors.add(new AssertionError(e));
    }
  }

  private void awaitUp(final String host) throws TimeoutException {
    final TaskStatus status = Polling.awaitUnchecked(
        TIMEOUT_MILLIS, MILLISECONDS, new Callable<TaskStatus>() {
          @Override
          public TaskStatus call() throws Exception {
            final JobStatus status = Futures.getUnchecked(client.jobStatus(job.getId()));
            if (status == null) {
              return null;
            }
            final TaskStatus taskStatus = status.getTaskStatuses().get(host);
            if (taskStatus == null) {
              return null;
            }

            return taskStatus.getState() == TaskStatus.State.RUNNING ? taskStatus : null;
          }
        }
    );

    statuses.put(host, status);

    for (final String port : waitPorts) {
      awaitPort(port, host);
    }
  }

  private void awaitPort(final String port, final String host) throws TimeoutException {
    final TaskStatus taskStatus = statuses.get(host);
    assert taskStatus != null;
    final Integer externalPort = taskStatus.getPorts().get(port).getExternalPort();
    assert externalPort != null;
    Polling.awaitUnchecked(TIMEOUT_MILLIS, MILLISECONDS, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        log.info("Probing: {} @ {}:{}", port, host, externalPort);
        final boolean up = prober.probe(host, externalPort);
        if (up) {
          log.info("Up: {} @ {}:{}", port, host, externalPort);
          return true;
        } else {
          return null;
        }
      }
    });
  }

  private static <T> T get(final ListenableFuture<T> future)
      throws InterruptedException, ExecutionException, TimeoutException {
    return future.get(TIMEOUT_MILLIS, MILLISECONDS);
  }

  public static interface Deployer {

    TemporaryJob deploy(Job job, List<String> hosts, Set<String> waitPorts);
  }
}
