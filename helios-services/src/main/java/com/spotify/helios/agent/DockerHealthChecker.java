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

package com.spotify.helios.agent;

import com.google.common.annotations.VisibleForTesting;

import com.spotify.helios.servicescommon.RiemannFacade;
import com.spotify.helios.servicescommon.statistics.MeterRates;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;
import com.codahale.metrics.health.HealthCheck;

import io.dropwizard.lifecycle.Managed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Health checker exposed by /healthcheck on the admin port (5804 by default).
 */
public class DockerHealthChecker extends HealthCheck implements Managed {
  private static final Logger log = LoggerFactory.getLogger(DockerHealthChecker.class);

  @VisibleForTesting
  static final double FAILURE_LOW_WATERMARK = 0.4;
  @VisibleForTesting
  static final double FAILURE_HIGH_WATERMARK = 0.8;

  private final SupervisorMetrics metrics;
  private final ScheduledExecutorService scheduler;
  private final TimeUnit timeUnit;
  private final int interval;
  private final HealthCheckRunnable runnable;
  private final RiemannFacade facade;

  public DockerHealthChecker(final SupervisorMetrics metrics,
                             final TimeUnit timeUnit, int interval,
                             final RiemannFacade facade) {
    super();
    this.metrics = checkNotNull(metrics);
    this.timeUnit = checkNotNull(timeUnit);
    this.facade = checkNotNull(facade).stack("docker-health");

    this.scheduler = Executors.newScheduledThreadPool(1);
    this.interval = interval;
    this.runnable = new HealthCheckRunnable();
  }

  private class HealthCheckRunnable implements Runnable {
    private String reason = null;

    @Override
    public void run() {
      final double timeoutRatio = fiveMinuteRatio(
          metrics.getDockerTimeoutRates(), metrics.getSupervisorRunRates());
      final double exceptionRatio = fiveMinuteRatio(
          metrics.getContainersThrewExceptionRates(), metrics.getSupervisorRunRates());
      log.info("timeout ratio is {}, exception ratio is {}", timeoutRatio, exceptionRatio);

      final String origReason = reason;

      // Yay hysteresis!
      if (timeoutRatio > FAILURE_HIGH_WATERMARK) {
        reason = "docker timeouts are too high for too long";
      }
      if (exceptionRatio > FAILURE_HIGH_WATERMARK) {
        reason = "supervisor run exception frequency is too high";
      }

      if (timeoutRatio < FAILURE_LOW_WATERMARK && exceptionRatio < FAILURE_LOW_WATERMARK) {
        reason = null;
      }

      // If reason changed, emit an event
      if (origReason != null && reason == null) {
        facade.event()
            .state("ok")
            .tags("docker", "health")
            .metric(1)
            .send();
      } else if (reason != null && origReason == null) {
        facade.event()
            .state("critical")
            .tags("docker", "health")
            .metric(0)
            .send();
      }
    }
  }

  private double fiveMinuteRatio(MeterRates numerator, MeterRates denominator) {
    if (denominator.getFiveMinuteRate() < .1) {
      return 0.0;
    }
    return (numerator.getFiveMinuteRate() * 1.0) / denominator.getFiveMinuteRate();
  }

  @Override
  public void stop() {
    scheduler.shutdownNow();
  }

  @Override
  public void start() {
    scheduler.scheduleAtFixedRate(runnable, interval, interval, timeUnit);
  }

  @Override
  protected Result check() throws Exception {
    runnable.run();
    if (runnable.reason != null) {
      return Result.unhealthy(runnable.reason);
    } else {
      return Result.healthy();
    }
  }
}
