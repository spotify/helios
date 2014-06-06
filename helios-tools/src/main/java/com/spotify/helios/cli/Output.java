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

import org.joda.time.Duration;
import org.joda.time.Period;

import java.io.PrintStream;

public class Output {

  public static String humanDuration(final long millis) {
    return humanDuration(Duration.millis(millis));
  }

  public static String humanDuration(final Duration d) {
    final Period p = d.toPeriod().normalizedStandard();

    if (d.getStandardSeconds() == 0) {
      return "0 seconds";
    } else if (d.getStandardSeconds() < 60) {
      return p.getSeconds() + " seconds";
    } else if (d.getStandardMinutes() < 60) {
      return p.getMinutes() + " minutes " + p.getSeconds() + " seconds";
    } else if (d.getStandardHours() < 24) {
      return p.getHours() + " hours " + p.getMinutes() + " minutes";
    } else {
      return d.getStandardDays() + " days " + p.getHours() + " hours";
    }
  }

  public static Table table(final PrintStream out) {
    return new Table(out);
  }

  public static JobStatusTable jobStatusTable(final PrintStream out, final boolean full) {
    return new JobStatusTable(out, full);
  }
}
