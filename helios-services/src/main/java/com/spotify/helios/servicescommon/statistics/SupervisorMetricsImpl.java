package com.spotify.helios.servicescommon.statistics;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

public class SupervisorMetricsImpl implements SupervisorMetrics {
  private static final String TYPE = "agent_supervisor";

  private final RequestMetrics imagePull;

  private final Counter containerStartedCounter;
  private final Counter containersExitedCounter;
  private final Counter containersRunningCounter;
  private final Counter containersThrewExceptionCounter;
  private final Counter imageCacheHitCounter;
  private final Counter supervisorClosedCounter;
  private final Counter supervisorStartedCounter;
  private final Counter supervisorStoppedCounter;
  private final Counter dockerTimeoutCounter;

  private final MetricName containerStarted;
  private final MetricName containersExited;
  private final MetricName containersRunning;
  private final MetricName containersThrewException;
  private final MetricName dockerTimeout;
  private final MetricName imageCacheHit;
  private final MetricName supervisorClosed;
  private final MetricName supervisorStarted;
  private final MetricName supervisorStopped;

  public SupervisorMetricsImpl(final String group,
                               final MetricsRegistry registry) {

    containerStarted = new MetricName(group, TYPE, "container_started");
    containersExited = new MetricName(group, TYPE, "containers_exited");
    containersRunning = new MetricName(group, TYPE, "containers_running");
    containersThrewException = new MetricName(group, TYPE, "containers_threw_exception");
    imageCacheHit = new MetricName(group, TYPE, "image_cache_hit");
    supervisorClosed = new MetricName(group, TYPE, "supervisor_closed");
    supervisorStarted = new MetricName(group, TYPE, "supervisors_created");
    supervisorStopped = new MetricName(group, TYPE, "supervisor_stopped");
    dockerTimeout = new MetricName(group, TYPE, "docker_timeout");

    containerStartedCounter = registry.newCounter(containerStarted);
    containersExitedCounter = registry.newCounter(containersExited);
    containersRunningCounter = registry.newCounter(containersRunning);
    containersThrewExceptionCounter = registry.newCounter(containersThrewException);
    imageCacheHitCounter = registry.newCounter(imageCacheHit);
    supervisorClosedCounter = registry.newCounter(supervisorClosed);
    supervisorStartedCounter = registry.newCounter(supervisorStarted);
    supervisorStoppedCounter = registry.newCounter(supervisorStopped);
    dockerTimeoutCounter = registry.newCounter(dockerTimeout);

    imagePull = new RequestMetrics(group, TYPE, "image_pull", registry);
  }

  @Override
  public void supervisorStarted() {
    supervisorStartedCounter.inc();
  }

  @Override
  public void supervisorStopped() {
    supervisorStoppedCounter.inc();
  }

  @Override
  public void supervisorClosed() {
    supervisorClosedCounter.inc();
  }

  @Override
  public void containersRunning() {
    containersRunningCounter.inc();
  }

  @Override
  public void containersExited() {
    containersExitedCounter.inc();
  }

  @Override
  public void containersThrewException() {
    containersThrewExceptionCounter.inc();
  }

  @Override
  public void containerStarted() {
    containerStartedCounter.inc();
  }

  @Override
  public MetricsContext containerPull() {
    return new MetricsContextImpl(imagePull);
  }

  @Override
  public void imageCacheHit() {
      imageCacheHitCounter.inc();
  }

  @Override
  public void dockerTimeout() {
    dockerTimeoutCounter.inc();
  }

}
