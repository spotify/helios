/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios;

import com.google.common.base.Throwables;

import org.junit.Ignore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

@Ignore
public class PortAllocator {

  private static final AtomicInteger PORT_COUNTER = new AtomicInteger(30000);

  @SuppressWarnings("ThrowFromFinallyBlock")
  public static int allocatePort() {
    while (true) {
      final int port = PORT_COUNTER.incrementAndGet();
      if (port > 65535) {
        PORT_COUNTER.set(30000);
        return allocatePort();
      }
      Socket s = null;
      try {
        s = new Socket();
        s.bind(new InetSocketAddress("127.0.0.1", port));
        return port;
      } catch (IOException ignore) {
      } finally {
        try {
          if (s != null) {
            s.close();
          }
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    }
  }
}
