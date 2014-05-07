/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.testing;

import java.io.IOException;
import java.net.Socket;

class DefaultProber implements Prober {

  @Override
  public boolean probe(final String host, final int port) {
    try (final Socket ignored = new Socket(host, port)) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
