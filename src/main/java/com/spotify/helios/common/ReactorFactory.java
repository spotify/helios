/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

public class ReactorFactory {

  public Reactor create(final Runnable callback, final long timeout) {
    return new Reactor(callback, timeout);
  }
}
