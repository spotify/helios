/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.coordination;

import com.spotify.helios.common.descriptors.JobId;

public class Paths {

  private static final String UP = "up";
  public static final String CONFIG = "config";
  public static final String STATUS = "status";
  public static final String JOBS = "jobs";
  public static final String AGENTS = "agents";
  public static final String MASTERS = "masters";
  public static final String HISTORY = "history";
  private static final String HOSTINFO = "hostinfo";
  private static final String RUNTIMEINFO = "runtimeinfo";

  private static final PathFactory CONFIG_JOBS = new PathFactory("/", CONFIG, JOBS);
  private static final PathFactory CONFIG_AGENTS = new PathFactory("/", CONFIG, AGENTS);
  private static final PathFactory STATUS_AGENTS = new PathFactory("/", STATUS, AGENTS);
  private static final PathFactory STATUS_MASTERS = new PathFactory("/", STATUS, MASTERS);
  private static final PathFactory HISTORY_JOBS = new PathFactory("/", HISTORY, JOBS);

  public static String configAgents() {
    return CONFIG_AGENTS.path();
  }

  public static String configJobs() {
    return CONFIG_JOBS.path();
  }

  public static String configJob(final JobId id) {
    return CONFIG_JOBS.path(id.toString());
  }

  public static String configJobAgents(final JobId jobId) {
    return CONFIG_JOBS.path(jobId.toString(), AGENTS);
  }

  public static String configJobAgent(final JobId jobId, final String agent) {
    return CONFIG_JOBS.path(jobId.toString(), AGENTS, agent);
  }

  public static String configAgent(final String agent) {
    return CONFIG_AGENTS.path(agent);
  }

  public static String configAgentJobs(final String agent) {
    return CONFIG_AGENTS.path(agent, JOBS);
  }

  public static String configAgentJob(final String agent, final JobId jobId) {
    return CONFIG_AGENTS.path(agent, JOBS, jobId.toString());
  }

  public static String statusAgent(final String agent) {
    return STATUS_AGENTS.path(agent);
  }

  public static String statusAgentJobs(final String agent) {
    return STATUS_AGENTS.path(agent, JOBS);
  }

  public static String statusAgentJob(final String agent, final JobId jobId) {
    return STATUS_AGENTS.path(agent, JOBS, jobId.toString());
  }

  public static String statusAgentUp(final String agent) {
    return STATUS_AGENTS.path(agent, UP);
  }

  public static String statusMasterUp(final String master) {
    return STATUS_MASTERS.path(master, UP);
  }

  public static String statusMaster() {
    return STATUS_MASTERS.path();
  }

  public static String statusAgentHostInfo(final String agent) {
    return STATUS_AGENTS.path(agent, HOSTINFO);
  }

  public static String statusAgentRuntimeInfo(final String agent) {
    return STATUS_AGENTS.path(agent, RUNTIMEINFO);
  }

  public static String historyJobAgent(final String job, final String agent, final long timestamp) {
    return HISTORY_JOBS.path(job, agent + ":" + String.valueOf(timestamp));
  }

  public static String historyJob(final String job) {
    return HISTORY_JOBS.path(job);
  }
}
