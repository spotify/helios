/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.base.Predicate;

public class PortCounter {

  private int i;

  private final int start;
  private final int end;

  public PortCounter(final int start, final int end) {
    this.start = start;
    this.end = end;
    this.i = start;
  }

  public Integer findPort(final Predicate<Integer> predicate) {
    for (int i = start; i < end; i++) {
      final int port = next();
      if (predicate.apply(port)) {
        return port;
      }
    }
    return null;
  }

  private int next() {
    if (i == end) {
      i = start;
    }
    return i++;
  }
}
