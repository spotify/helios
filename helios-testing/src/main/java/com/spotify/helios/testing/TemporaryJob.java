/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.testing;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;

import org.apache.commons.lang.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.newHashMap;
import static com.spotify.helios.testing.Jobs.TIMEOUT_MILLIS;
import static com.spotify.helios.testing.Jobs.get;
import static com.spotify.helios.testing.Jobs.getJobDescription;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.fail;

public class TemporaryJob {

  private static final Logger log = LoggerFactory.getLogger(TemporaryJob.class);

  private final Map<String, TaskStatus> statuses = newHashMap();
  private final HeliosClient client;
  private final Prober prober;
  private final Job job;
  private final List<String> hosts;
  private final Map<String, String> hostToIp = newHashMap();
  private final Set<String> waitPorts;
  private final String jobDeployedMessageFormat;
  private final long deployTimeoutMillis;

  TemporaryJob(final HeliosClient client, final Prober prober,
               final Job job,
               final List<String> hosts, final Set<String> waitPorts,
               final String jobDeployedMessageFormat, final long deployTimeoutMillis) {
    this.client = checkNotNull(client, "client");
    this.prober = checkNotNull(prober, "prober");
    this.job = checkNotNull(job, "job");
    this.hosts = ImmutableList.copyOf(checkNotNull(hosts, "hosts"));
    this.waitPorts = ImmutableSet.copyOf(checkNotNull(waitPorts, "waitPorts"));
    this.jobDeployedMessageFormat = Optional.fromNullable(jobDeployedMessageFormat).or("");
    this.deployTimeoutMillis = deployTimeoutMillis;
  }

  public Job job() {
    return job;
  }

  public List<String> hosts() {
    return hosts;
  }

  public Map<String, TaskStatus> statuses() {
    return ImmutableMap.copyOf(statuses);
  }

  /**
   * Returns the port that a job can be reached at given the host and name of registered port.
   * This is useful to discover the value of a dynamically allocated port.
   * @param host the host where the job is deployed
   * @param port the name of the registered port
   * @return the port where the job can be reached, or null if the host or port name is not found
   */
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

  /**
   * Returns a {@link com.google.common.net.HostAndPort} for a registered port. This is useful
   * for discovering the value of dynamically allocated ports. This method should only be called
   * when the job has been deployed to a single host. If the job has been deployed to multiple
   * hosts an AssertionError will be thrown indicating that the {@link #addresses(String)} method
   * should must  called instead.
   * @param port the name of the registered port
   * @return a HostAndPort describing where the registered port can be reached. Null if
   * no ports have been registered.
   * @throws java.lang.AssertionError if the job has been deployed to more than one host
   */
  public HostAndPort address(final String port) {
    final List<HostAndPort> addresses = addresses(port);

    if (addresses.size() > 1) {
      throw new AssertionError(
          "Job has been deployed to multiple hosts, use addresses method instead");
    }

    return addresses.get(0);
  }

  /**
   * Returns a {@link com.google.common.net.HostAndPort} object for a registered port, for each
   * host the job has been deployed to. This is useful for discovering the value of dynamically
   * allocated ports.
   * @param port the name of the registered port
   * @return a HostAndPort describing where the registered port can be reached. Null if
   * no ports have been registered.
   */
  public List<HostAndPort> addresses(final String port) {
    checkArgument(job.getPorts().containsKey(port), "port %s not found", port);
    final List<HostAndPort> addresses = Lists.newArrayList();
    for (final Map.Entry<String, TaskStatus> entry : statuses.entrySet()) {
      final Integer externalPort = entry.getValue().getPorts().get(port).getExternalPort();
      assert externalPort != null;
      final String host = endpointFromHost(entry.getKey());
      addresses.add(HostAndPort.fromParts(host, externalPort));
    }
    return addresses;
  }

  void deploy() {
    try {
      // Create job
      log.info("Creating job {}", job.getId().toShortString());
      final CreateJobResponse createResponse = get(client.createJob(job));
      if (createResponse.getStatus() != CreateJobResponse.Status.OK) {
        fail(format("Failed to create job %s - %s", job.getId(),
                    createResponse.toString()));
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      fail(format("Failed to create job %s %s - %s", job.getId(), job.toString(), e));
    }

    try {
      // Deploy job
      final Deployment deployment = Deployment.of(job.getId(), Goal.START);
      for (final String host : hosts) {
        // HELIOS_HOST_ADDRESS is the IP address we should use to reach the host, instead of
        // the hostname. This is used when running a helios cluster inside a VM, and the containers
        // can be reached by IP address only, since DNS won't be able to resolve the host name of
        // the helios agent running in the VM.
        final HostStatus hostStatus = client.hostStatus(host).get();
        final String hostAddress = hostStatus.getEnvironment().get("HELIOS_HOST_ADDRESS");
        if (hostAddress != null) {
          hostToIp.put(host, hostAddress);
        }

        log.info("Deploying {} to {}", getJobDescription(job), host);
        final JobDeployResponse deployResponse = get(client.deploy(deployment, host));
        if (deployResponse.getStatus() != JobDeployResponse.Status.OK) {
          fail(format("Failed to deploy job %s %s - %s",
                      job.getId(), job.toString(), deployResponse));
        }
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      fail(format("Failed to deploy job %s %s - %s", job.getId(), job.toString(), e));
    }

    try {
      // Wait for job to come up
      for (final String host : hosts) {
        awaitUp(host);
      }
    } catch (TimeoutException e) {
      fail(format("Failed while probing job %s %s - %s", job.getId(), job.toString(), e));
    }
  }

  void undeploy(final List<AssertionError> errors) {
    Jobs.undeploy(client, job, hosts, errors);
  }

  /**
   * Undeploys and removes this TemporaryJob from the Helios cluster. This is normally done
   * automatically by TemporaryJobs at the end of the test run. Use this method if you need to
   * manually undeploy a job prior to the end of the test run.
   */
  public void undeploy() {
    final List<AssertionError> errors = Lists.newArrayList();
    undeploy(errors);

    if (errors.size() > 0) {
      fail(format("Failed to undeploy job %s - %s",
                  getJobDescription(job), errors.get(0)));
    }
  }
  
  private void awaitUp(final String host) throws TimeoutException {
    final AtomicBoolean messagePrinted = new AtomicBoolean(false);
    final TaskStatus status = Polling.awaitUnchecked(
      deployTimeoutMillis, MILLISECONDS, new Callable<TaskStatus>() {
        @Override
        public TaskStatus call() throws Exception {
          final JobStatus status = Futures.getUnchecked(client.jobStatus(job.getId()));
          if (status == null) {
            log.debug("Job status not available");
            return null;
          }
          final TaskStatus taskStatus = status.getTaskStatuses().get(host);
          if (taskStatus == null) {
            log.debug("Task status not available on {}", host);
            return null;
          }

          if (!messagePrinted.get() &&
              !isNullOrEmpty(jobDeployedMessageFormat) &&
              !isNullOrEmpty(taskStatus.getContainerId())) {
            outputDeployedMessage(host, taskStatus.getContainerId());
            messagePrinted.set(true);
          }

          verifyHealthy(host, taskStatus);

          final TaskStatus.State state = taskStatus.getState();
          log.info("Job state of {}: {}", job.getImage(), state);

          if (state == TaskStatus.State.RUNNING) {
            return taskStatus;
          }

          return null;
        }
      }
    );

    statuses.put(host, status);

    for (final String port : waitPorts) {
      awaitPort(port, host);
    }
  }

  void verifyHealthy() throws AssertionError {
    log.debug("Checking health of {}", job.getImage());
    final JobStatus status = Futures.getUnchecked(client.jobStatus(job.getId()));
    if (status == null) {
      return;
    }
    for (final Map.Entry<String, TaskStatus> entry : status.getTaskStatuses().entrySet()) {
      verifyHealthy(entry.getKey(), entry.getValue());
    }
  }

  private void verifyHealthy(final String host, final TaskStatus status) {
    log.debug("Checking health of {} on {}", job.getImage(), host);
    final TaskStatus.State state = status.getState();
    if (state == TaskStatus.State.FAILED ||
        state == TaskStatus.State.EXITED ||
        state == TaskStatus.State.STOPPED) {
      // Throw exception which should stop the test dead in it's tracks
      String stateString = state.toString();
      if (status.getThrottled() != ThrottleState.NO) {
        stateString += format("(%s)", status.getThrottled());
      }
      throw new AssertionError(format(
          "Unexpected job state %s for job %s with image %s on host %s. Check helios agent "
          + "logs for details. If you're using HeliosSoloDeployment, set "
          + "`HeliosSoloDeployment.fromEnv().removeHeliosSoloOnExit(false)` and check the"
          + "logs of the helios-solo container with `docker logs <container ID>`.",
          stateString, job.getId().toShortString(), job.getImage(), host));
    }
  }

  private void awaitPort(final String port, final String host) throws TimeoutException {
    final String endpoint = endpointFromHost(host);
    final TaskStatus taskStatus = statuses.get(host);
    assert taskStatus != null;
    final PortMapping portMapping = taskStatus.getPorts().get(port);
    final Integer externalPort = portMapping.getExternalPort();
    assert externalPort != null;
    Polling.awaitUnchecked(TIMEOUT_MILLIS, MILLISECONDS, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        log.info("Probing: {} @ {}:{}", port, endpoint, portMapping);
        final boolean up = prober.probe(endpoint, portMapping);
        if (up) {
          log.info("Up: {} @ {}:{}", port, endpoint, externalPort);
          return true;
        } else {
          return null;
        }
      }
    });
  }

  /**
   * Returns the ip address mapped to the given hostname. If no mapping exists, the hostname is
   * returned.
   * @param host the hostname to look up
   * @return The host's ip address if one exists, otherwise the hostname which was passed in.
   */
  private String endpointFromHost(String host) {
    final String ip = hostToIp.get(host);
    return ip == null ? host : ip;
  }

  private void outputDeployedMessage(final String host, final String containerId) {
    final StrSubstitutor subst = new StrSubstitutor(new ImmutableMap.Builder<String, Object>()
        .put("host", host)
        .put("name", job.getId().getName())
        .put("version", job.getId().getVersion())
        .put("hash", job.getId().getHash())
        .put("job", job.toString())
        .put("containerId", containerId)
        .build()
        );
    log.info("{}", subst.replace(jobDeployedMessageFormat));
  }
}
