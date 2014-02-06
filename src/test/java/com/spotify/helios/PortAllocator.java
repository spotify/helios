/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios;

import com.google.common.base.Throwables;

import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ThreadLocalRandom;

@Ignore
public class PortAllocator {

  final static Logger log = LoggerFactory.getLogger(PortAllocator.class);

  @SuppressWarnings("ThrowFromFinallyBlock")
  public static int allocatePort(final String name) {
    while (true) {
      final int port = ThreadLocalRandom.current().nextInt(49152, 65536);
      Socket s = null;
      try {
        s = new Socket();
        s.bind(new InetSocketAddress("127.0.0.1", port));
        log.debug("allocated port \"{}\": {}", name, port);
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
