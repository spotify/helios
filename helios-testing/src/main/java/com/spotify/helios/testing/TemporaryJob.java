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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.spotify.helios.testing.Jobs.getJobDescription;
import static java.lang.String.format;
import static org.junit.Assert.fail;

public class TemporaryJob {

  private final Map<String, TaskStatus> statuses;
  private final HeliosClient client;
  private final Job job;
  private final List<String> hosts;
  private final Map<String, String> hostToIp;

  TemporaryJob(final HeliosClient client,
               final Job job,
               final List<String> hosts,
               final Map<String, String> hostToIp,
               final Map<String, TaskStatus> statuses) {
    this.client = checkNotNull(client, "client");
    this.job = checkNotNull(job, "job");
    this.hosts = ImmutableList.copyOf(checkNotNull(hosts, "hosts"));
    this.hostToIp = ImmutableMap.copyOf(checkNotNull(hostToIp, "hostToIp"));
    this.statuses = ImmutableMap.copyOf(checkNotNull(statuses, "statuses"));
  }

  public Job job() {
    return job;
  }

  public List<String> hosts() {
    return hosts;
  }

  public Map<String, TaskStatus> statuses() {
    return statuses;
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
}
