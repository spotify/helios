package com.spotify.helios.servicescommon.statistics;

public class NoopMetrics implements Metrics {

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public MasterMetrics getMasterMetrics() {
    return new NoopMasterMetrics();
  }

  @Override
  public SupervisorMetrics getSupervisorMetrics() {
    return new NoopSupervisorMetrics();
  }

}
