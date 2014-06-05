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
    return new MeterRates(0, 0, 0);
  }

  @Override
  public MeterRates getContainersThrewExceptionRates() {
    return new MeterRates(0, 0, 0);
  }

  @Override
  public MeterRates getSupervisorRunRates() {
    return new MeterRates(0, 0, 0);
  }
}
