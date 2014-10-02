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
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

public class RequestMetrics {

  private final Counter successCounter;
  private final Counter failureCounter;
  private final Counter userErrorCounter;
  private final Timer timer;

  public RequestMetrics(final String group, final String type, final String requestName,
                        final MetricRegistry registry) {

    final String prefix = MetricRegistry.name(group, type, requestName);
    successCounter = registry.counter(prefix + "_successful");
    failureCounter = registry.counter(prefix + "_failed");
    userErrorCounter = registry.counter(prefix + "_failed");
    timer = registry.timer(prefix + "_latency");
  }

  public Context begin() {
    return timer.time();
  }

  public void success(Context context) {
    success();
    context.stop();
  }

  public void success() {
    successCounter.inc();
  }

  public void failure(Context context) {
    failure();
    context.stop();
  }

  public void failure() {
    failureCounter.inc();
  }

  public void userError(Context context) {
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
}
