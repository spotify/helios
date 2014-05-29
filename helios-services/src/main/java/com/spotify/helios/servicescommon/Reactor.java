/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.servicescommon;

import com.google.common.util.concurrent.Service;

public interface Reactor extends Service {

  interface Callback {
    void run(boolean timeout) throws InterruptedException;
  }

  /**
   * Send a signal to trigger the reactor. Does not block.
   */
  void signal();

  /**
   * Returns a runnable that calls {@link #signal()}
   */
  Runnable signalRunnable();
}
