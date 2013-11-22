/**
 * Copyright (C) 2013 Spotify AB
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
}
