package com.spotify.helios.servicescommon.statistics;

public interface SupervisorMetrics {

  public void supervisorStarted();

  public void supervisorStopped();

  public void supervisorClosed();

  public void containersRunning();

  public void containersExited();

  public void containersThrewException();

  public void containerStarted();

  public MetricsContext containerPull();

  public void imageCacheHit();

}
