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

package com.spotify.helios.servicescommon;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Tracks API requests to the master to be tracked by the yammer metrics stuff.
 */
public class MasterRequestMetrics {

  private final Counter successCounter;
  private final Counter failureCounter;
  private final Counter userErrorCounter;

  private final Meter successMeter;
  private final Meter failureMeter;
  private final Meter userErrorMeter;

  public MasterRequestMetrics(String group, String type, String requestName,
                              final MetricRegistry registry) {
    final String prefix = name(group, type, requestName);

    successCounter = registry.counter(prefix + "_count_success");
    failureCounter = registry.counter(prefix + "_count_failures");
    userErrorCounter = registry.counter(prefix + "_count_usererror");

    successMeter = registry.meter(prefix + "_meter_success");
    failureMeter = registry.meter(prefix + "_meter_failures");
    userErrorMeter = registry.meter(prefix + "_meter_usererror");
  }

  public void success() {
    successCounter.inc();
    successMeter.mark();
  }

  public void failure() {
    failureCounter.inc();
    failureMeter.mark();
  }

  public void userError() {
    userErrorCounter.inc();
    userErrorMeter.mark();
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
}
