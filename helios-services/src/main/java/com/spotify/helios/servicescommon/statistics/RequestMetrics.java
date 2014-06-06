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
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.util.concurrent.TimeUnit;

public class RequestMetrics {

  private final Counter successCounter;
  private final Counter failureCounter;
  private final Counter userErrorCounter;
  private final Timer timer;
  private final MetricName successName;
  private final MetricName failureName;
  private final MetricName userErrorName;
  private final MetricName timerName;

  public RequestMetrics(final String group, final String type, final String requestName,
                        final MetricsRegistry registry) {

    successName = new MetricName(group, type, requestName + "_successful");
    failureName = new MetricName(group, type, requestName + "_failed");
    userErrorName = new MetricName(group, type, requestName + "_failed");
    timerName = new MetricName(group, type, requestName + "_latency");

    successCounter = registry.newCounter(successName);
    failureCounter = registry.newCounter(failureName);
    userErrorCounter = registry.newCounter(userErrorName);
    timer = registry.newTimer(timerName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  }

  public TimerContext begin() {
    return timer.time();
  }

  public void success(TimerContext context) {
    success();
    context.stop();
  }

  public void success() {
    successCounter.inc();
  }

  public void failure(TimerContext context) {
    failure();
    context.stop();
  }

  public void failure() {
    failureCounter.inc();
  }

  public void userError(TimerContext context) {
    userError();
    context.stop();
  }

  public void userError() {
    userErrorCounter.inc();
  }

  public Counter getSuccessCounter() {
    return successCounter;
  }

  public Counter getFailureCounter() {
    return failureCounter;
  }

  public Counter getUserErrorCounter() {
    return failureCounter;
  }

  public Timer getTimer() {
    return timer;
  }

  public MetricName getSuccessName() {
    return successName;
  }

  public MetricName getFailureName() {
    return failureName;
  }

  public MetricName getTimerName() {
    return timerName;
  }

  public MetricName getUserErrorName() {
    return userErrorName;
  }
}
