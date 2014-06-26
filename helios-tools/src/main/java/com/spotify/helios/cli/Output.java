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
import org.xbill.DNS.Name;
import org.xbill.DNS.ResolverConfig;
import org.xbill.DNS.TextParseException;

import java.io.PrintStream;

import static java.lang.String.format;

public class Output {

  public static String humanDuration(final long millis) {
    return humanDuration(Duration.millis(millis));
  }

  public static String humanDuration(final Duration d) {
    final Period p = d.toPeriod().normalizedStandard();

    if (d.getStandardSeconds() == 0) {
      return "0 seconds";
    } else if (d.getStandardSeconds() < 60) {
      return format("%d second%s", p.getSeconds(), p.getSeconds() > 1 ? "s" : "");
    } else if (d.getStandardMinutes() < 60) {
      return format("%d minute%s", p.getMinutes(), p.getMinutes() > 1 ? "s" : "");
    } else if (d.getStandardHours() < 24) {
      return format("%d hour%s", p.getHours(), p.getHours() > 1 ? "s" : "");
    } else {
      return format("%d day%s", d.getStandardDays(), d.getStandardDays() > 1 ? "s" : "");
    }
  }

  public static Table table(final PrintStream out) {
    return new Table(out);
  }

  public static JobStatusTable jobStatusTable(final PrintStream out, final boolean full) {
    return new JobStatusTable(out, full);
  }

  public static String shortHostname(final String host) {
    final Name root = Name.fromConstantString(".");
    final Name hostname;
    try {
      hostname = Name.fromString(host, root);
    } catch (TextParseException e) {
      throw new IllegalArgumentException("Invalid hostname '" + host + "'");
    }
    for (Name domain : ResolverConfig.getCurrentConfig().searchPath()) {
      if (hostname.subdomain(domain)) {
        return hostname.relativize(domain).toString();
      }
    }
    return hostname.toString();
  }

  public static String formatHostname(final boolean full, final String host) {
    return full ? host : shortHostname(host);
  }
}
