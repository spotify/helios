/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.servicescommon.coordination;

import com.spotify.helios.common.descriptors.JobId;

public class Paths {

  private static final String UP = "up";
  public static final String CONFIG = "config";
  public static final String STATUS = "status";
  public static final String JOBS = "jobs";
  public static final String JOBREFS = "jobrefs";
  public static final String HOSTS = "hosts";
  public static final String EVENTS = "events";
  public static final String MASTERS = "masters";
  public static final String HISTORY = "history";
  private static final String HOSTINFO = "hostinfo";
  private static final String AGENTINFO = "agentinfo";
  private static final String PORTS = "ports";
  private static final String ENVIRONMENT = "environment";
  private static final String ID = "id";

  private static final PathFactory CONFIG_JOBS = new PathFactory("/", CONFIG, JOBS);
  private static final PathFactory CONFIG_JOBREFS = new PathFactory("/", CONFIG, JOBREFS);
  private static final PathFactory CONFIG_HOSTS = new PathFactory("/", CONFIG, HOSTS);
  private static final PathFactory STATUS_HOSTS = new PathFactory("/", STATUS, HOSTS);
  private static final PathFactory STATUS_MASTERS = new PathFactory("/", STATUS, MASTERS);
  private static final PathFactory HISTORY_JOBS = new PathFactory("/", HISTORY, JOBS);

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

  public static String configHostPorts(final String host) {
    return CONFIG_HOSTS.path(host, PORTS);
  }

  public static String configHostPort(final String host, final int port) {
    return CONFIG_HOSTS.path(host, PORTS, String.valueOf(port));
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
    return STATUS_HOSTS.path(host, JOBS, jobId.toString());
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

  public static String historyJobHostEventsTimestamp(final JobId jobId,
                                                     final String host,
                                                     final long timestamp) {
    return HISTORY_JOBS.path(jobId.toString(), HOSTS, host, EVENTS, String.valueOf(timestamp));
  }

  public static String historyJobHostEvents(final JobId jobId, final String host) {
    return HISTORY_JOBS.path(jobId.toString(), HOSTS, host, EVENTS);
  }

  public static String historyJobHosts(final JobId jobId) {
    return HISTORY_JOBS.path(jobId.toString(), HOSTS);
  }

  public static String historyJob(final JobId jobId) {
    return HISTORY_JOBS.path(jobId.toString());
  }

  public static String historyJobs() {
    return HISTORY_JOBS.path();
  }
}
