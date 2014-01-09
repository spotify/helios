package com.spotify.helios.common.statistics;

public interface Metrics {

  public void start();

  public void stop();

  public MasterMetrics getMasterMetrics();

  public SupervisorMetrics getSupervisorMetrics();

}
