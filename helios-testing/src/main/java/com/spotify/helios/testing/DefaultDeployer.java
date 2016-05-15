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

import com.google.common.base.Joiner;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.fail;

class DefaultDeployer implements Deployer {

  private static final Logger log = LoggerFactory.getLogger(DefaultDeployer.class);

  private final HeliosClient client;
  private final Set<String> hosts;
  private final List<TemporaryJob> jobs;
  private final String jobDeployedMessageFormat;
  private final long deployTimeoutMillis;
  private final Path tempJobDir;

  private boolean readyToDeploy;

  DefaultDeployer(final HeliosClient client, final Set<String> hosts,
                  final List<TemporaryJob> jobs,
                  final String jobDeployedMessageFormat, final long deployTimeoutMillis,
                  final Path tempJobDir) {
    this.client = checkNotNull(client, "client");
    this.jobs = checkNotNull(jobs, "jobs");
    this.hosts = checkNotNull(hosts, "hosts");
    System.out.println("BBB HOSTS: " + hosts.toString());
    this.jobDeployedMessageFormat = jobDeployedMessageFormat;
    this.deployTimeoutMillis = deployTimeoutMillis;
    this.tempJobDir = checkNotNull(tempJobDir, "tempJobDir");
  }

  @Override
  public TemporaryJob deploy(final Job job, final Set<String> waitPorts,
                             final Prober prober,
                             final TemporaryJobReports.ReportWriter reportWriter) {
    if (!readyToDeploy) {
      fail("deploy() must be called in a @Before or in the test method, or perhaps you forgot"
           + " to put @Rule before TemporaryJobs");
    }

    if (hosts.isEmpty()) {
      // TODO (dxia) comment seems inaccurate
      fail("at least one host must be explicitly specified, or deploy() must be called with " +
           "no arguments to automatically select a host");
    }

    log.info("Deploying {} to {}", job.getImage(), Joiner.on(", ").skipNulls().join(hosts));
    final TemporaryJob temporaryJob = new TemporaryJob(client, prober, reportWriter, job, hosts,
                                                       waitPorts, jobDeployedMessageFormat,
                                                       deployTimeoutMillis, tempJobDir);
    jobs.add(temporaryJob);
    temporaryJob.deploy();
    return temporaryJob;
  }

  @Override
  public void readyToDeploy() {
    readyToDeploy = true;
  }
}
