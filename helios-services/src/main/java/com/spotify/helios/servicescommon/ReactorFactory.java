/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.servicescommon;

import static com.spotify.helios.servicescommon.Reactor.Callback;

public class ReactorFactory {

  public Reactor create(final String name, final Callback callback, final long timeout) {
    return new DefaultReactor(name, callback, timeout);
  }
}
