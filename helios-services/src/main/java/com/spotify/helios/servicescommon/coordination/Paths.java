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
  private static final String LABELS = "labels";
  private static final String ID = "id";
  private static final String DEPLOYMENT_GROUPS = "deployment-groups";
  private static final String DEPLOYMENT_GROUP_TASKS = "deployment-group-tasks";

  private static final PathFactory CONFIG_ID = new PathFactory("/", CONFIG, ID);
  private static final PathFactory CONFIG_JOBS = new PathFactory("/", CONFIG, JOBS);
  private static final PathFactory CONFIG_JOBREFS = new PathFactory("/", CONFIG, JOBREFS);
  private static final PathFactory CONFIG_HOSTS = new PathFactory("/", CONFIG, HOSTS);
  private static final PathFactory CONFIG_DEPLOYMENT_GROUPS = new PathFactory(
      "/", CONFIG, DEPLOYMENT_GROUPS);

  private static final PathFactory STATUS_HOSTS = new PathFactory("/", STATUS, HOSTS);
  private static final PathFactory STATUS_MASTERS = new PathFactory("/", STATUS, MASTERS);
  private static final PathFactory STATUS_DEPLOYMENT_GROUPS = new PathFactory(
      "/", STATUS, DEPLOYMENT_GROUPS);
  private static final PathFactory STATUS_DEPLOYMENT_GROUP_TASKS = new PathFactory(
      "/", STATUS, DEPLOYMENT_GROUP_TASKS);

  private static final PathFactory HISTORY_JOBS = new PathFactory("/", HISTORY, JOBS);
  private static final String CREATION_PREFIX = "creation-";

  public static String configHosts() {
    return CONFIG_HOSTS.path();
  }

  public static String configJobs() {
    return CONFIG_JOBS.path();
  }

  public static String configJobRefs() {
    return CONFIG_JOBREFS.path();
  }

  public static String configJob(final JobId id) {
    return CONFIG_JOBS.path(id.toString());
  }

  public static String configDeploymentGroups() {
    return CONFIG_DEPLOYMENT_GROUPS.path();
  }

  public static String configDeploymentGroup(final String name) {
    return CONFIG_DEPLOYMENT_GROUPS.path(name);
  }

  public static boolean isConfigJobCreation(final String child) {
    return child.startsWith(CREATION_PREFIX);
  }

  public static UUID configJobCreationId(final String child) {
    return UUID.fromString(child.substring(CREATION_PREFIX.length()));
  }

  public static String configHostJobCreationParent(final JobId id) {
    return configJob(id);
  }

  public static String configJobCreation(final JobId id, final UUID operationId) {
    final String name = CREATION_PREFIX + operationId;
    return CONFIG_JOBS.path(id.toString(), name);
  }

  public static String configJobRefShort(final JobId id) {
    return CONFIG_JOBREFS.path(id.getName() + ":" + id.getVersion());
  }

  public static String configJobHosts(final JobId jobId) {
    return CONFIG_JOBS.path(jobId.toString(), HOSTS);
  }

  public static String configJobHost(final JobId jobId, final String host) {
    return CONFIG_JOBS.path(jobId.toString(), HOSTS, host);
  }

  public static String configHost(final String host) {
    return CONFIG_HOSTS.path(host);
  }

  public static String configHostId(final String host) {
    return CONFIG_HOSTS.path(host, ID);
  }

  public static String configHostJobs(final String host) {
    return CONFIG_HOSTS.path(host, JOBS);
  }

  public static String configHostJob(final String host, final JobId jobId) {
    return CONFIG_HOSTS.path(host, JOBS, jobId.toString());
  }

  public static String configHostJobCreation(final String host, final JobId id,
                                             final UUID operationId) {
    return CONFIG_HOSTS.path(host, JOBS, id.toString(), CREATION_PREFIX + operationId);
  }

  public static String configHostPorts(final String host) {
    return CONFIG_HOSTS.path(host, PORTS);
  }

  public static String configHostPort(final String host, final int port) {
    return CONFIG_HOSTS.path(host, PORTS, String.valueOf(port));
  }

  public static String configId(final String id) {
    return CONFIG_ID.path(id);
  }

  public static String statusHosts() {
    return STATUS_HOSTS.path();
  }

  public static String statusHost(final String host) {
    return STATUS_HOSTS.path(host);
  }

  public static String statusHostJobs(final String host) {
    return STATUS_HOSTS.path(host, JOBS);
  }

  public static String statusHostJob(final String host, final JobId jobId) {
    return statusHostJob(host, jobId.toString());
  }

  public static String statusHostJob(final String host, final String jobIdString) {
    return STATUS_HOSTS.path(host, JOBS, jobIdString);
  }

  public static String statusHostUp(final String host) {
    return STATUS_HOSTS.path(host, UP);
  }

  public static String statusMasterUp(final String master) {
    return STATUS_MASTERS.path(master, UP);
  }

  public static String statusMasters() {
    return STATUS_MASTERS.path();
  }

  public static String statusMaster() {
    return STATUS_MASTERS.path();
  }

  public static String statusHostInfo(final String host) {
    return STATUS_HOSTS.path(host, HOSTINFO);
  }

  public static String statusHostAgentInfo(final String host) {
    return STATUS_HOSTS.path(host, AGENTINFO);
  }

  public static String statusHostEnvVars(final String host) {
    return STATUS_HOSTS.path(host, ENVIRONMENT);
  }

  public static String statusHostLabels(final String host) {
    return STATUS_HOSTS.path(host, LABELS);
  }

  public static String statusDeploymentGroups() {
    return STATUS_DEPLOYMENT_GROUPS.path();
  }

  public static String statusDeploymentGroup(final String name) {
    return STATUS_DEPLOYMENT_GROUPS.path(name);
  }

  public static String statusDeploymentGroupHosts(final String name) {
    return STATUS_DEPLOYMENT_GROUPS.path(name, HOSTS);
  }

  public static String statusDeploymentGroupTasks() {
    return STATUS_DEPLOYMENT_GROUP_TASKS.path();
  }

  public static String statusDeploymentGroupTasks(final String deploymentGroupName) {
    return STATUS_DEPLOYMENT_GROUP_TASKS.path(deploymentGroupName);
  }

  public static String historyJobHostEventsTimestamp(final JobId jobId,
                                                     final String host,
                                                     final long timestamp) {
    return HISTORY_JOBS.path(jobId.toString(), HOSTS, host, EVENTS, String.valueOf(timestamp));
  }

  public static String historyJobHostEvents(final JobId jobId, final String host) {
    return historyJobHostEvents(jobId.toString(), host);
  }

  public static String historyJobHostEvents(final String jobIdString, final String host) {
    return HISTORY_JOBS.path(jobIdString, HOSTS, host, EVENTS);
  }

  public static String historyJobHosts(final JobId jobId) {
    return HISTORY_JOBS.path(jobId.toString(), HOSTS);
  }

  public static String historyJobHost(final JobId jobId, final String host) {
    return HISTORY_JOBS.path(jobId.toString(), HOSTS, host);
  }

  public static String historyJob(final JobId jobId) {
    return HISTORY_JOBS.path(jobId.toString());
  }

  public static String historyJobs() {
    return HISTORY_JOBS.path();
  }
}
