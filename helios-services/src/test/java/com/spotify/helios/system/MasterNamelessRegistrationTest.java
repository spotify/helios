/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.spotify.nameless.api.EndpointFilter;
import com.spotify.nameless.api.NamelessClient;
import com.spotify.nameless.proto.Messages;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MasterNamelessRegistrationTest extends NamelessTestBase {
  @Test
  public void test() throws Exception {
    startMaster("-vvvv",
                "--no-log-setup",
                "--munin-port", "0",
                "--site", "localhost",
                "--http", "0.0.0.0:" + EXTERNAL_PORT,
                "--hm", masterEndpoint,
                "--zk", zookeeperEndpoint);

    // sleep for half a second to give master time to register with nameless
    Thread.sleep(500);
    NamelessClient client = new NamelessClient(hermesClient("tcp://localhost:4999"));
    List<Messages.RegistryEntry> entries = client.queryEndpoints(EndpointFilter.everything()).get();

    assertEquals("wrong number of nameless entries", 2, entries.size());

    boolean httpFound = false;
    boolean hermesFound = false;

    for (Messages.RegistryEntry entry : entries) {
      final Messages.Endpoint endpoint = entry.getEndpoint();
      assertEquals("wrong service", "helios", endpoint.getService());
      final String protocol = endpoint.getProtocol();

      switch (protocol) {
        case "hm":
          hermesFound = true;
          assertEquals("wrong port", endpoint.getPort(), masterPort);
          break;
        case "http":
          httpFound = true;
          assertEquals("wrong port", endpoint.getPort(), EXTERNAL_PORT);
          break;
        default:
          fail("unknown protocol " + protocol);
      }
    }

    assertTrue("missing hermes nameless entry", hermesFound);
    assertTrue("missing http nameless entry", httpFound);
  }
}
