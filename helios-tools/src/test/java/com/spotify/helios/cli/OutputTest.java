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

package com.spotify.helios.cli;

import org.junit.Test;

import static com.spotify.helios.cli.Output.humanDuration;
import static org.hamcrest.Matchers.is;
import static org.joda.time.Duration.standardDays;
import static org.joda.time.Duration.standardHours;
import static org.joda.time.Duration.standardMinutes;
import static org.joda.time.Duration.standardSeconds;
import static org.junit.Assert.assertThat;

public class OutputTest {

  @Test
  public void testHumanDuration() throws Exception {
    assertThat(humanDuration(standardSeconds(0)), is("0 seconds"));

    assertThat(humanDuration(standardSeconds(1)), is("1 second"));
    assertThat(humanDuration(standardSeconds(2)), is("2 seconds"));
    assertThat(humanDuration(standardSeconds(59)), is("59 seconds"));
    assertThat(humanDuration(standardSeconds(60)), is("1 minute"));

    assertThat(humanDuration(standardMinutes(1)), is("1 minute"));
    assertThat(humanDuration(standardMinutes(2)), is("2 minutes"));
    assertThat(humanDuration(standardMinutes(59)), is("59 minutes"));
    assertThat(humanDuration(standardMinutes(60)), is("1 hour"));

    assertThat(humanDuration(standardHours(1)), is("1 hour"));
    assertThat(humanDuration(standardHours(2)), is("2 hours"));
    assertThat(humanDuration(standardHours(23)), is("23 hours"));
    assertThat(humanDuration(standardHours(24)), is("1 day"));

    assertThat(humanDuration(standardDays(1)), is("1 day"));
    assertThat(humanDuration(standardDays(2)), is("2 days"));
    assertThat(humanDuration(standardDays(365)), is("365 days"));
    assertThat(humanDuration(standardDays(4711)), is("4711 days"));
  }
}