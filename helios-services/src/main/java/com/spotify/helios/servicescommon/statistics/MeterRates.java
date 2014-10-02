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

import com.codahale.metrics.Meter;

public class MeterRates {
  private final double oneMinuteRate;
  private final double fiveMinuteRate;
  private final double fifteenMinuteRate;

  public MeterRates(Meter meter) {
    this(meter.getOneMinuteRate(), meter.getFiveMinuteRate(), meter.getFifteenMinuteRate());
  }

  public MeterRates(double oneMinuteRate, double fiveMinuteRate, double fifteenMinuteRate) {
    this.oneMinuteRate = oneMinuteRate;
    this.fiveMinuteRate = fiveMinuteRate;
    this.fifteenMinuteRate = fifteenMinuteRate;
  }

  public double getOneMinuteRate() {
    return oneMinuteRate;
  }

  public double getFiveMinuteRate() {
    return fiveMinuteRate;
  }

  public double getFifteenMinuteRate() {
    return fifteenMinuteRate;
  }
}
