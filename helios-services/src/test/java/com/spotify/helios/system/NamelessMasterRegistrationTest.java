/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.spotify.nameless.api.EndpointFilter;
import com.spotify.nameless.proto.Messages;

import org.junit.Test;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Callable;

import static com.spotify.nameless.proto.Messages.RegistryEntry;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NamelessMasterRegistrationTest extends NamelessTestBase {

  @Test
  public void test() throws Exception {
    startMaster("-vvvv",
                "--no-log-setup",
                "--site", "localhost",
                "--http", masterEndpoint,
                "--admin=" + masterAdminPort,
                "--zk", zk.connectString(),
                "--nameless=" + namelessEndpoint);
    final int masterPort = URI.create(masterEndpoint).getPort();

    // Wait for the master to get registered with nameless
    final List<RegistryEntry> entries = await(
        LONG_WAIT_MINUTES, MINUTES, new Callable<List<RegistryEntry>>() {
      @Override
      public List<RegistryEntry> call() throws Exception {
        final List<RegistryEntry> entries =
            namelessClient.queryEndpoints(EndpointFilter.everything()).get();
        return entries.size() == 1 ? entries : null;
      }
    });

    boolean httpFound = false;

    for (RegistryEntry entry : entries) {
      final Messages.Endpoint endpoint = entry.getEndpoint();
      assertEquals("wrong service", "helios", endpoint.getService());
      final String protocol = endpoint.getProtocol();

      switch (protocol) {
        case "http":
          httpFound = true;
          assertEquals("wrong port", endpoint.getPort(), masterPort);
          break;
        default:
          fail("unknown protocol " + protocol);
      }
    }

    assertTrue("missing http nameless entry", httpFound);
  }
}
