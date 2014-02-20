/**
 * Copyright © 2006-2012 Spotify AB
 */

package com.spotify.helios.agent;

/**
 * Provides retry schedulers. See {@link BoundedRandomExponentialBackoff}.
 */
public interface RetryIntervalPolicy {

  RetryScheduler newScheduler();
}
