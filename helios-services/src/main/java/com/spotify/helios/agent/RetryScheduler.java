/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.agent;

public interface RetryScheduler {

  /**
   * Get the current interval. Updated when {@link #nextMillis()} is called.
   *
   * @return The current interval in milliseconds.
   */
  long currentMillis();

  /**
   * Progress to and return a new interval. {@link #currentMillis()} will return this interval until
   * the next invocation of {@link #nextMillis()}.
   *
   * @return The new interval in milliseconds.
   */
  long nextMillis();
}
