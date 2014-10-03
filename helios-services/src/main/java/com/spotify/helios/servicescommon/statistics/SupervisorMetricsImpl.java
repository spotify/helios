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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

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


  public SupervisorMetricsImpl(final String group,
                               final MetricRegistry registry) {

    final String prefix = MetricRegistry.name(group, TYPE) + ".";

    containerStartedCounter = registry.counter(prefix + "container_started_counter");
    containersExitedCounter = registry.counter(prefix + "containers_exited_counter");
    containersRunningCounter = registry.counter(prefix + "containers_running_counter");
    containersThrewExceptionCounter = registry.counter(
        prefix + "containers_threw_exception_counter");
    imageCacheHitCounter = registry.counter(prefix + "image_cache_hit_counter");
    supervisorClosedCounter = registry.counter(prefix + "supervisor_closed_counter");
    supervisorStartedCounter = registry.counter(prefix + "supervisors_created_counter");
    supervisorStoppedCounter = registry.counter(prefix + "supervisor_stopped_counter");
    supervisorRunCounter = registry.counter(prefix + "supervisor_run_counter");
    dockerTimeoutCounter = registry.counter(prefix + "docker_timeout_counter");

    containerStartedMeter = registry.meter(prefix + "container_started_meter");
    containersExitedMeter = registry.meter(prefix + "containers_exited_meter");
    containersRunningMeter = registry.meter(prefix + "containers_running_meter");
    containersThrewExceptionMeter = registry.meter(prefix + "containers_threw_exception_meter");
    imageCacheHitMeter = registry.meter(prefix + "image_cache_hit_meter");
    supervisorClosedMeter = registry.meter(prefix + "supervisor_closed_meter");
    supervisorStartedMeter = registry.meter(prefix + "supervisors_created_meter");
    supervisorStoppedMeter = registry.meter(prefix + "supervisor_stopped_meter");
    supervisorRunMeter = registry.meter(prefix + "supervisor_run_meter");
    dockerTimeoutMeter = registry.meter(prefix + "docker_timeout_meter");

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
