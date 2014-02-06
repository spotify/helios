package com.spotify.helios.servicescommon.statistics;

public interface Metrics {

  public void start();

  public void stop();

  public MasterMetrics getMasterMetrics();

  public SupervisorMetrics getSupervisorMetrics();

}
