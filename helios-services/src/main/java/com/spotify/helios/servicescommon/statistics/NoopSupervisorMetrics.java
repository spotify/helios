package com.spotify.helios.servicescommon.statistics;


public class NoopSupervisorMetrics implements SupervisorMetrics {

  @Override
  public void supervisorStarted() {}

  @Override
  public void supervisorStopped() {}

  @Override
  public void supervisorClosed() {}

  @Override
  public void containersRunning() {}

  @Override
  public void containersExited() {}

  @Override
  public void containersThrewException() {}

  @Override
  public void containerStarted() {}

  @Override
  public MetricsContext containerPull() {
    return new NoopMetricsContext();
  }

  @Override
  public void imageCacheHit() {}
}
