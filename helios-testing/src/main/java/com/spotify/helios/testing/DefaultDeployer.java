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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.HostStatus.Status;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;

import org.apache.commons.lang.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.containsPattern;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.helios.testing.Jobs.TIMEOUT_MILLIS;
import static com.spotify.helios.testing.Jobs.get;
import static com.spotify.helios.testing.Jobs.getJobDescription;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.fail;

class DefaultDeployer implements Deployer {

  private static final Logger log = LoggerFactory.getLogger(DefaultDeployer.class);

  private final HeliosClient client;
  private final List<TemporaryJob> jobs;
  private final HostPickingStrategy hostPicker;
  private final String jobDeployedMessageFormat;
  private final long deployTimeoutMillis;
  private final Undeployer undeployer;

  DefaultDeployer(final HeliosClient client,
                  final List<TemporaryJob> jobs,
                  final HostPickingStrategy hostPicker,
                  final String jobDeployedMessageFormat,
                  final long deployTimeoutMillis,
                  final Undeployer undeployer) {
    this.client = checkNotNull(client, "client");
    this.jobs = checkNotNull(jobs, "jobs");
    this.hostPicker = checkNotNull(hostPicker, "hostPicker");
    this.jobDeployedMessageFormat = jobDeployedMessageFormat;
    this.deployTimeoutMillis = deployTimeoutMillis;
    this.undeployer = checkNotNull(undeployer, "undeployer");
  }

  @Override
  public TemporaryJob deploy(final Job job, final String hostFilter, final Set<String> waitPorts,
                             final Prober prober) {
    if (isNullOrEmpty(hostFilter)) {
      fail("a host filter pattern must be passed to hostFilter(), " +
           "or one must be specified in HELIOS_HOST_FILTER");
    }

    final List<String> hosts;
    try {
      log.info("Getting list of hosts");
      hosts = client.listHosts().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new AssertionError("Failed to get list of Helios hosts", e);
    }

    final List<String> filteredHosts = FluentIterable.from(hosts)
        .filter(containsPattern(hostFilter))
        .toList();

    log.info("Got this filtered list of hosts with host filter '{}': {}",
             hostFilter, filteredHosts);

    if (filteredHosts.isEmpty()) {
      fail(format("no hosts matched the filter pattern - %s", hostFilter));
    }

    final String chosenHost = pickHost(filteredHosts);
    return deploy(job, Collections.singletonList(chosenHost), waitPorts, prober);
  }

  @VisibleForTesting
  String pickHost(final List<String> filteredHosts) {
    final List<String> mutatedList = Lists.newArrayList(filteredHosts);
    
    while (true) {
      final String candidateHost = hostPicker.pickHost(mutatedList);
      try {
        final HostStatus hostStatus = client.hostStatus(candidateHost).get();
        if (hostStatus != null && Status.UP == hostStatus.getStatus()) {
          return candidateHost;
        } 
        mutatedList.remove(candidateHost);
        if (mutatedList.isEmpty()) {
          fail("all hosts matching filter pattern are DOWN");
        }
      } catch (InterruptedException | ExecutionException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  @Override
  public TemporaryJob deploy(final Job job, final List<String> hosts, final Set<String> waitPorts,
                             final Prober prober) {
    if (hosts.isEmpty()) {
      fail("at least one host must be explicitly specified, or deploy() must be called with " +
           "no arguments to automatically select a host");
    }

    log.info("Deploying {} to {}", job.getImage(), Joiner.on(", ").skipNulls().join(hosts));
    final TemporaryJob temporaryJob = doDeploy(job, hosts, waitPorts, prober);
    jobs.add(temporaryJob);
    return temporaryJob;
  }

  private TemporaryJob doDeploy(final Job job, final List<String> hosts,
                                final Set<String> waitPorts, final Prober prober) {
    final Map<String, String> hostToIp = new HashMap<>();
    final Map<String, TaskStatus> statuses = new HashMap<>();

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
        awaitUp(job, host, statuses, waitPorts, prober, hostToIp);
      }
    } catch (TimeoutException e) {
      fail(format("Failed while probing job %s %s - %s", job.getId(), job.toString(), e));
    }

    return new TemporaryJob(job, hosts, hostToIp, statuses, undeployer);
  }

  private void awaitUp(final Job job, final String host,
                       final Map<String, TaskStatus> statuses,
                       final Set<String> waitPorts,
                       final Prober prober,
                       final Map<String, String> hostToIp) throws TimeoutException {
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
            outputDeployedMessage(job, host, taskStatus.getContainerId());
            messagePrinted.set(true);
          }

          Jobs.verifyHealthy(job, client, host, taskStatus);

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
      awaitPort(port, host, statuses, prober, hostToIp);
    }
  }

  private void awaitPort(final String port, final String host,
                         final Map<String, TaskStatus> statuses,
                         final Prober prober,
                         final Map<String, String> hostToIp) throws TimeoutException {
    final String endpoint = endpointFromHost(host, hostToIp);
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

  private void outputDeployedMessage(final Job job, final String host, final String containerId) {
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

  private String endpointFromHost(final String host, final Map<String, String> hostToIp) {
    final String ip = hostToIp.get(host);
    return ip == null ? host : ip;
  }

}
