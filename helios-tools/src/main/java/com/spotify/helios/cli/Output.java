/*-
 * -\-\-
 * Helios Tools
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.cli;

import static java.lang.String.format;

import java.io.PrintStream;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.xbill.DNS.Name;
import org.xbill.DNS.ResolverConfig;
import org.xbill.DNS.TextParseException;

public class Output {

  public static String humanDuration(final long millis) {
    return humanDuration(Duration.millis(millis));
  }

  public static String humanDuration(final Duration dur) {
    final Period p = dur.toPeriod().normalizedStandard();

    if (dur.getStandardSeconds() == 0) {
      return "0 seconds";
    } else if (dur.getStandardSeconds() < 60) {
      return format("%d second%s", p.getSeconds(), p.getSeconds() > 1 ? "s" : "");
    } else if (dur.getStandardMinutes() < 60) {
      return format("%d minute%s", p.getMinutes(), p.getMinutes() > 1 ? "s" : "");
    } else if (dur.getStandardHours() < 24) {
      return format("%d hour%s", p.getHours(), p.getHours() > 1 ? "s" : "");
    } else {
      return format("%d day%s", dur.getStandardDays(), dur.getStandardDays() > 1 ? "s" : "");
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

    final ResolverConfig currentConfig = ResolverConfig.getCurrentConfig();
    if (currentConfig != null) {
      final Name[] searchPath = currentConfig.searchPath();
      if (searchPath != null) {
        for (final Name domain : searchPath) {
          if (hostname.subdomain(domain)) {
            return hostname.relativize(domain).toString();
          }
        }
      }
    }
    return hostname.toString();
  }

  public static String formatHostname(final boolean full, final String host) {
    return full ? host : shortHostname(host);
  }
}
