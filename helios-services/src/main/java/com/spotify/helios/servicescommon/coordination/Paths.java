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

package com.spotify.helios.servicescommon.coordination;

import com.spotify.helios.common.descriptors.JobId;

import java.util.UUID;

public class Paths {
  private static final String UP = "up";
  private static final String CONFIG = "config";
  private static final String STATUS = "status";
  private static final String JOBS = "jobs";
  private static final String JOBREFS = "jobrefs";
  private static final String HOSTS = "hosts";
  private static final String EVENTS = "events";
  private static final String MASTERS = "masters";
  private static final String HISTORY = "history";
  private static final String HOSTINFO = "hostinfo";
  private static final String AGENTINFO = "agentinfo";
  private static final String PORTS = "ports";
  private static final String ENVIRONMENT = "environment";
  private static final String ID = "id";
  private static final String CREATION_PREFIX = "creation-";

  private final PathFactory configJobs;
  private final PathFactory configJobRefs;
  private final PathFactory configHosts;
  private final PathFactory statusHosts;
  private final PathFactory statusMasters;
  private final PathFactory historyJobs;

  public Paths(final String prefix) {
    configJobs = new PathFactory(prefix, CONFIG, JOBS);
    configJobRefs = new PathFactory(prefix, CONFIG, JOBREFS);
    configHosts = new PathFactory(prefix, CONFIG, HOSTS);
    statusHosts = new PathFactory(prefix, STATUS, HOSTS);
    statusMasters = new PathFactory(prefix, STATUS, MASTERS);
    historyJobs = new PathFactory(prefix, HISTORY, JOBS);
  }

  public String configHosts() {
    return configHosts.path();
  }

  public String configJobs() {
    return configJobs.path();
  }

  public String configJobRefs() {
    return configJobRefs.path();
  }

  public String configJob(final JobId id) {
    return configJobs.path(id.toString());
  }

  public boolean isConfigJobCreation(final JobId id, final String parent,
                                     final String child) {
    return child.startsWith(CREATION_PREFIX);
  }

  public UUID configJobCreationId(final JobId id, final String parent, final String child) {
    return UUID.fromString(child.substring(CREATION_PREFIX.length()));
  }

  public String configHostJobCreationParent(final JobId id) {
    return configJob(id);
  }

  public String configJobCreation(final JobId id, final UUID operationId) {
    final String name = CREATION_PREFIX + operationId;
    return configJobs.path(id.toString(), name);
  }

  public String configJobRefShort(final JobId id) {
    return configJobRefs.path(id.getName() + ":" + id.getVersion());
  }

  public String configJobHosts(final JobId jobId) {
    return configJobs.path(jobId.toString(), HOSTS);
  }

  public String configJobHost(final JobId jobId, final String host) {
    return configJobs.path(jobId.toString(), HOSTS, host);
  }

  public String configHost(final String host) {
    return configHosts.path(host);
  }

  public String configHostId(final String host) {
    return configHosts.path(host, ID);
  }

  public String configHostJobs(final String host) {
    return configHosts.path(host, JOBS);
  }

  public String configHostJob(final String host, final JobId jobId) {
    return configHosts.path(host, JOBS, jobId.toString());
  }

  public String configHostJobCreation(final String host, final JobId id,
                                             final UUID operationId) {
    return configHosts.path(host, JOBS, id.toString(), CREATION_PREFIX + operationId);
  }

  public String configHostPorts(final String host) {
    return configHosts.path(host, PORTS);
  }

  public String configHostPort(final String host, final int port) {
    return configHosts.path(host, PORTS, String.valueOf(port));
  }

  public String statusHosts() {
    return statusHosts.path();
  }

  public String statusHost(final String host) {
    return statusHosts.path(host);
  }

  public String statusHostJobs(final String host) {
    return statusHosts.path(host, JOBS);
  }

  public String statusHostJob(final String host, final JobId jobId) {
    return statusHosts.path(host, JOBS, jobId.toString());
  }

  public String statusHostUp(final String host) {
    return statusHosts.path(host, UP);
  }

  public String statusMasterUp(final String master) {
    return statusMasters.path(master, UP);
  }

  public String statusMasters() {
    return statusMasters.path();
  }

  public String statusMaster() {
    return statusMasters.path();
  }

  public String statusHostInfo(final String host) {
    return statusHosts.path(host, HOSTINFO);
  }

  public String statusHostAgentInfo(final String host) {
    return statusHosts.path(host, AGENTINFO);
  }

  public String statusHostEnvVars(final String host) {
    return statusHosts.path(host, ENVIRONMENT);
  }

  public String historyJobHostEventsTimestamp(final JobId jobId,
                                                     final String host,
                                                     final long timestamp) {
    return historyJobs.path(jobId.toString(), HOSTS, host, EVENTS, String.valueOf(timestamp));
  }

  public String historyJobHostEvents(final JobId jobId, final String host) {
    return historyJobs.path(jobId.toString(), HOSTS, host, EVENTS);
  }

  public String historyJobHosts(final JobId jobId) {
    return historyJobs.path(jobId.toString(), HOSTS);
  }

  public String historyJobHost(final JobId jobId, final String host) {
    return historyJobs.path(jobId.toString(), HOSTS, host);
  }

  public String historyJob(final JobId jobId) {
    return historyJobs.path(jobId.toString());
  }

  public String historyJobs() {
    return historyJobs.path();
  }
}
