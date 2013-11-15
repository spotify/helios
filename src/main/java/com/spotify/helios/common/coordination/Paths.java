/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.coordination;

public class Paths {

  private static final String UP = "up";
  public static final String CONFIG = "config";
  public static final String STATUS = "status";
  public static final String JOBS = "jobs";
  public static final String AGENTS = "agents";
  public static final String MASTERS = "masters";

  private static final PathFactory CONFIG_JOBS = new PathFactory("/", CONFIG, JOBS);
  private static final PathFactory CONFIG_AGENTS = new PathFactory("/", CONFIG, AGENTS);
  private static final PathFactory STATUS_AGENTS = new PathFactory("/", STATUS, AGENTS);
  private static final PathFactory STATUS_MASTERS = new PathFactory("/", STATUS, MASTERS);

  public static String configAgents() {
    return CONFIG_AGENTS.path();
  }

  public static String configJobs() {
    return CONFIG_JOBS.path();
  }

  public static String configJob(final String id) {
    return CONFIG_JOBS.path(id);
  }

  public static String configAgent(final String agent) {
    return CONFIG_AGENTS.path(agent);
  }

  public static String configAgentJobs(final String agent) {
    return CONFIG_AGENTS.path(agent, JOBS);
  }

  public static String configAgentJob(final String agent, final String job) {
    return CONFIG_AGENTS.path(agent, JOBS, job);
  }

  public static String configJobPath(final String id) {
    return CONFIG_JOBS.path(id);
  }

  public static String statusAgent(final String agent) {
    return STATUS_AGENTS.path(agent);
  }

  public static String statusAgentJobs(final String agent) {
    return STATUS_AGENTS.path(agent, JOBS);
  }

  public static String statusAgentJob(final String agent, final String jobId) {
    return STATUS_AGENTS.path(agent, JOBS, jobId);
  }

  public static String upMaster(final String master) {
    return STATUS_MASTERS.path(master, UP);
  }

  public static String statusMasters() {
    return STATUS_MASTERS.path();
  }
}
