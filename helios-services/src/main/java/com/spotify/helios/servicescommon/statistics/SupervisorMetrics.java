package com.spotify.helios.servicescommon.statistics;

public interface SupervisorMetrics {

  void supervisorStarted();

  void supervisorStopped();

  void supervisorClosed();

  void containersRunning();

  void containersExited();

  void containersThrewException();

  void containerStarted();

  MetricsContext containerPull();

  void imageCacheHit();

  void dockerTimeout();

  void supervisorRun();

  MeterRates getDockerTimeoutRates();
  MeterRates getContainersThrewExceptionRates();
  MeterRates getSupervisorRunRates();
}
