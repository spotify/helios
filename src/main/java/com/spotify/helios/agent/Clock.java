package com.spotify.helios.agent;

import org.joda.time.Instant;

public interface Clock {
  public Instant now();
}
