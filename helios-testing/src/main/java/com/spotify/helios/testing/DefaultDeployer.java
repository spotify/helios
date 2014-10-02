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

package com.spotify.helios.testing;

import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Predicates.containsPattern;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.fail;

public class DefaultDeployer implements Deployer {

  private static final Logger log = LoggerFactory.getLogger(DefaultDeployer.class);

  private final HeliosClient client;
  private final List<TemporaryJob> jobs;

  private boolean readyToDeploy;

  public DefaultDeployer(HeliosClient client, List<TemporaryJob> jobs) {
    this.client = client;
    this.jobs = jobs;
  }

  @Override
  public TemporaryJob deploy(final Job job, final String hostFilter,
                             final Set<String> waitPorts, final Prober prober) {
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

    if (filteredHosts.isEmpty()) {
      fail(format("no hosts matched the filter pattern - %s", hostFilter));
    }

    final String chosenHost = filteredHosts.get(new Random().nextInt(filteredHosts.size()));
    return deploy(job, asList(chosenHost), waitPorts, prober);
  }

  @Override
  public TemporaryJob deploy(final Job job, final List<String> hosts,
                             final Set<String> waitPorts, final Prober prober) {
    if (!readyToDeploy) {
      fail("deploy() must be called in a @Before or in the test method, or perhaps you forgot"
           + " to put @Rule before TemporaryJobs");
    }

    if (hosts.isEmpty()) {
      fail("at least one host must be explicitly specified, or deploy() must be called with " +
           "no arguments to automatically select a host");
    }

    log.info("Deploying {} to {}", job.getImage(), Joiner.on(", ").skipNulls().join(hosts));
    final TemporaryJob temporaryJob = new TemporaryJob(client, prober, job, hosts, waitPorts);
    jobs.add(temporaryJob);
    temporaryJob.deploy();
    return temporaryJob;
  }

  public void readyToDeploy() {
    readyToDeploy = true;
  }

}
