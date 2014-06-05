/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.servicescommon.statistics;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import static java.util.concurrent.TimeUnit.MINUTES;

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
  private final Counter supervisorRunCounter;
  private final Counter dockerTimeoutCounter;

  private final Meter containerStartedMeter;
  private final Meter containersExitedMeter;
  private final Meter containersRunningMeter;
  private final Meter containersThrewExceptionMeter;
  private final Meter dockerTimeoutMeter;
  private final Meter imageCacheHitMeter;
  private final Meter supervisorClosedMeter;
  private final Meter supervisorStartedMeter;
  private final Meter supervisorStoppedMeter;
  private final Meter supervisorRunMeter;

  private final MetricName containerStartedCounterName;
  private final MetricName containersExitedCounterName;
  private final MetricName containersRunningCounterName;
  private final MetricName containersThrewExceptionCounterName;
  private final MetricName dockerTimeoutCounterName;
  private final MetricName imageCacheHitCounterName;
  private final MetricName supervisorClosedCounterName;
  private final MetricName supervisorStartedCounterName;
  private final MetricName supervisorStoppedCounterName;
  private final MetricName supervisorRunCounterName;

  private final MetricName containerStartedMeterName;
  private final MetricName containersExitedMeterName;
  private final MetricName containersRunningMeterName;
  private final MetricName containersThrewExceptionMeterName;
  private final MetricName dockerTimeoutMeterName;
  private final MetricName imageCacheHitMeterName;
  private final MetricName supervisorClosedMeterName;
  private final MetricName supervisorStartedMeterName;
  private final MetricName supervisorStoppedMeterName;
  private final MetricName supervisorRunMeterName;

  public SupervisorMetricsImpl(final String group,
                               final MetricsRegistry registry) {

    containerStartedCounterName = new MetricName(group, TYPE, "container_started_counter");
    containersExitedCounterName = new MetricName(group, TYPE, "containers_exited_counter");
    containersRunningCounterName = new MetricName(group, TYPE, "containers_running_counter");
    containersThrewExceptionCounterName = new MetricName(group, TYPE,
                                                         "containers_threw_exception_counter");
    imageCacheHitCounterName = new MetricName(group, TYPE, "image_cache_hit_counter");
    supervisorClosedCounterName = new MetricName(group, TYPE, "supervisor_closed_counter");
    supervisorStartedCounterName = new MetricName(group, TYPE, "supervisors_created_counter");
    supervisorStoppedCounterName = new MetricName(group, TYPE, "supervisor_stopped_counter");
    supervisorRunCounterName = new MetricName(group, TYPE, "supervisor_run_counter");
    dockerTimeoutCounterName = new MetricName(group, TYPE, "docker_timeout_counter");

    containerStartedMeterName = new MetricName(group, TYPE, "container_started_meter");
    containersExitedMeterName = new MetricName(group, TYPE, "containers_exited_meter");
    containersRunningMeterName = new MetricName(group, TYPE, "containers_running_meter");
    containersThrewExceptionMeterName = new MetricName(group, TYPE,
                                                       "containers_threw_exception_meter");
    imageCacheHitMeterName = new MetricName(group, TYPE, "image_cache_hit_meter");
    supervisorClosedMeterName = new MetricName(group, TYPE, "supervisor_closed_meter");
    supervisorStartedMeterName = new MetricName(group, TYPE, "supervisors_created_meter");
    supervisorStoppedMeterName = new MetricName(group, TYPE, "supervisor_stopped_meter");
    supervisorRunMeterName = new MetricName(group, TYPE, "supervisor_run_meter");
    dockerTimeoutMeterName = new MetricName(group, TYPE, "docker_timeout_meter");

    containerStartedCounter = registry.newCounter(containerStartedCounterName);
    containersExitedCounter = registry.newCounter(containersExitedCounterName);
    containersRunningCounter = registry.newCounter(containersRunningCounterName);
    containersThrewExceptionCounter = registry.newCounter(containersThrewExceptionCounterName);
    imageCacheHitCounter = registry.newCounter(imageCacheHitCounterName);
    supervisorClosedCounter = registry.newCounter(supervisorClosedCounterName);
    supervisorStartedCounter = registry.newCounter(supervisorStartedCounterName);
    supervisorStoppedCounter = registry.newCounter(supervisorStoppedCounterName);
    supervisorRunCounter = registry.newCounter(supervisorRunCounterName);
    dockerTimeoutCounter = registry.newCounter(dockerTimeoutCounterName);

    containerStartedMeter = registry.newMeter(containerStartedMeterName, "container_starts",
        MINUTES);
    containersExitedMeter = registry.newMeter(containersExitedMeterName, "containers_exits",
        MINUTES);
    containersRunningMeter = registry.newMeter(containersRunningMeterName, "containers_ran",
        MINUTES);
    containersThrewExceptionMeter = registry.newMeter(containersThrewExceptionMeterName,
        "container_exceptions", MINUTES);
    imageCacheHitMeter = registry.newMeter(imageCacheHitMeterName, "image_cache_hits", MINUTES);
    supervisorClosedMeter = registry.newMeter(supervisorClosedMeterName, "supervisor_closes",
        MINUTES);
    supervisorStartedMeter = registry.newMeter(supervisorStartedMeterName, "supervisor_starts",
        MINUTES);
    supervisorStoppedMeter = registry.newMeter(supervisorStoppedMeterName, "supervisor_stops",
        MINUTES);
    supervisorRunMeter = registry.newMeter(supervisorRunMeterName, "supervisor_runs",
        MINUTES);
    dockerTimeoutMeter = registry.newMeter(dockerTimeoutMeterName, "docker_timeouts", MINUTES);

    imagePull = new RequestMetrics(group, TYPE, "image_pull", registry);
  }

  @Override
  public void supervisorStarted() {
    supervisorStartedCounter.inc();
    supervisorStartedMeter.mark();
  }

  @Override
  public void supervisorStopped() {
    supervisorStoppedCounter.inc();
    supervisorStoppedMeter.mark();
  }

  @Override
  public void supervisorClosed() {
    supervisorClosedCounter.inc();
    supervisorClosedMeter.mark();
  }

  @Override
  public void containersRunning() {
    containersRunningCounter.inc();
    containersRunningMeter.mark();
  }

  @Override
  public void containersExited() {
    containersExitedCounter.inc();
    containersExitedMeter.mark();
  }

  @Override
  public void containersThrewException() {
    containersThrewExceptionCounter.inc();
    containersThrewExceptionMeter.mark();
  }

  @Override
  public void containerStarted() {
    containerStartedCounter.inc();
    containerStartedMeter.mark();
  }

  @Override
  public MetricsContext containerPull() {
    return new MetricsContextImpl(imagePull);
  }

  @Override
  public void imageCacheHit() {
    imageCacheHitCounter.inc();
    imageCacheHitMeter.mark();
  }

  @Override
  public void imageCacheMiss() {
    // TODO (dano): implement
  }

  @Override
  public void dockerTimeout() {
    dockerTimeoutCounter.inc();
    dockerTimeoutMeter.mark();
  }

  @Override
  public MeterRates getDockerTimeoutRates() {
    return new MeterRates(dockerTimeoutMeter);
  }

  @Override
  public void supervisorRun() {
    supervisorRunCounter.inc();
    supervisorRunMeter.mark();
  }

  @Override
  public MeterRates getContainersThrewExceptionRates() {
    return new MeterRates(containersThrewExceptionMeter);
  }

  @Override
  public MeterRates getSupervisorRunRates() {
    return new MeterRates(supervisorRunMeter);
  }
}
