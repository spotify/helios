package com.spotify.helios.servicescommon.statistics;

public enum RequestType {
  JOB_CREATE("job_create", "Create Job"),
  JOB_GET("job_get", "Get Job"),
  JOB_LIST("job_list", "Job List"),
  JOB_DELETE("job_delete", "Delete Job"),
  AGENT_CREATE("agent_create", "Create Agent"),
  JOB_STATUS("job_status", "Job Status"),
  JOB_DEPLOY("job_deploy", "Deploy Job"),
  JOB_PATCH("job_patch", "Patch Job"),
  AGENT_GET_DEPLOYMENT("agent_get_deployment", "Get Agent Deployment"),
  AGENT_DELETE("agent_delete", "Delete Agent"),
  JOB_UNDEPLOY("undeploy", "Undeploy Job"),
  AGENT_STATUS_GET("agent_status_get", "Agent Status"),
  AGENT_GET("agent_get", "Get Agent Info"),
  MASTER_LIST("master_list", "List Masters"),
  JOB_HISTORY("job_history", "Job History");

  private final String metricsName, humanName;

  RequestType(final String metricsName, final String humanName) {
    this.metricsName = metricsName;
    this.humanName = humanName;
  }

  public String getHumanName() {
    return humanName;
  }

  public String getMetricsName() {
    return metricsName;
  }
}