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

import com.aphyr.riemann.Proto.Event;
import com.spotify.helios.servicescommon.CapturingRiemannClient;
import com.spotify.helios.servicescommon.statistics.MeterRates;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.spotify.helios.agent.DockerHealthChecker.FAILURE_HIGH_WATERMARK;
import static com.spotify.helios.agent.DockerHealthChecker.FAILURE_LOW_WATERMARK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class DockerHealthCheckerTest {
  private static final MeterRates OK_RATE = makeMeter(FAILURE_LOW_WATERMARK - .01);
  private static final MeterRates BETWEEN_RATE = makeMeter(FAILURE_HIGH_WATERMARK - .01);
  private static final MeterRates RUN_RATE = makeMeter(1);
  private static final MeterRates ZERO_RATE = makeMeter(0);
  private static final MeterRates BAD_RATE = makeMeter(FAILURE_HIGH_WATERMARK + .01);

  private SupervisorMetrics metrics;
  private DockerHealthChecker checker;
  private CapturingRiemannClient riemannClient;

  private static MeterRates makeMeter(double percentage) {
    int value = (int) (percentage * 100);
    return new MeterRates(value, value, value);
  }

  @Before
  public void setUp() throws Exception {
    riemannClient = new CapturingRiemannClient();
    metrics = Mockito.mock(SupervisorMetrics.class);
    checker = new DockerHealthChecker(metrics, TimeUnit.SECONDS, 1, riemannClient.facade());
  }

  @Test
  public void testTimeouts() throws Exception {
    when(metrics.getDockerTimeoutRates()).thenReturn(BAD_RATE); // start out as many timeouts
    when(metrics.getContainersThrewExceptionRates()).thenReturn(ZERO_RATE);
    when(metrics.getSupervisorRunRates()).thenReturn(RUN_RATE);

    checker.start();
    assertFalse(checker.check().isHealthy());
    checkForState("critical");

    when(metrics.getDockerTimeoutRates()).thenReturn(BETWEEN_RATE);
    Thread.sleep(2);
    assertFalse(checker.check().isHealthy());
    checkForNoEmission();

    when(metrics.getDockerTimeoutRates()).thenReturn(OK_RATE);
    assertTrue(checker.check().isHealthy());

    checkForState("ok");
  }

  @Test
  public void testExceptions() throws Exception {
    when(metrics.getDockerTimeoutRates()).thenReturn(ZERO_RATE);
    when(metrics.getContainersThrewExceptionRates()).thenReturn(BAD_RATE);
    when(metrics.getSupervisorRunRates()).thenReturn(RUN_RATE);

    checker.start();

    assertFalse(checker.check().isHealthy());
    checkForState("critical");

    when(metrics.getContainersThrewExceptionRates()).thenReturn(BETWEEN_RATE);
    assertFalse(checker.check().isHealthy());
    checkForNoEmission();

    when(metrics.getContainersThrewExceptionRates()).thenReturn(OK_RATE);
    assertTrue(checker.check().isHealthy());
    checkForState("ok");
  }

  @Test
  public void testBoth() throws Exception {
    when(metrics.getDockerTimeoutRates()).thenReturn(ZERO_RATE);
    when(metrics.getContainersThrewExceptionRates()).thenReturn(BAD_RATE);
    when(metrics.getSupervisorRunRates()).thenReturn(RUN_RATE);

    checker.start();

    assertFalse(checker.check().isHealthy());
    checkForState("critical");

    when(metrics.getContainersThrewExceptionRates()).thenReturn(BETWEEN_RATE);
    assertFalse(checker.check().isHealthy());
    checkForNoEmission();

    when(metrics.getContainersThrewExceptionRates()).thenReturn(OK_RATE);
    when(metrics.getDockerTimeoutRates()).thenReturn(BETWEEN_RATE); // between maintains unhealthy
    assertFalse(checker.check().isHealthy());
    checkForNoEmission();

    when(metrics.getDockerTimeoutRates()).thenReturn(OK_RATE);
    assertTrue(checker.check().isHealthy());
    checkForState("ok");
  }

  private void checkForState(String expectedState) {
    final List<Event> events = riemannClient.getEvents();
    assertFalse(events.isEmpty());
    final Event event = events.get(0);
    try {
      assertEquals(expectedState, event.getState());
    } finally {
      riemannClient.clearEvents();
    }
  }

  private void checkForNoEmission() {
    assertTrue(riemannClient.getEvents().isEmpty());
  }
}
