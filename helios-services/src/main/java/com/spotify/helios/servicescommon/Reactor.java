/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.servicescommon;

import com.google.common.util.concurrent.Service;

public interface Reactor extends Service {

  interface Callback {
    void run() throws InterruptedException;
  }

  void update();

  Runnable updateRunnable();
}
