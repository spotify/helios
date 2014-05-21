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

  @Override
  public void imageCacheMiss() {
  }

  @Override
  public void dockerTimeout() {}

  @Override
  public void supervisorRun() {}

  @Override
  public MeterRates getDockerTimeoutRates() {
    return new MeterRates(0,0,0);
  }

  @Override
  public MeterRates getContainersThrewExceptionRates() {
    return new MeterRates(0,0,0);
  }

  @Override
  public MeterRates getSupervisorRunRates() {
    return new MeterRates(0,0,0);
  }
}
