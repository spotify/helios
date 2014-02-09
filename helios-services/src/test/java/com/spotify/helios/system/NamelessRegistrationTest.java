/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.nameless.api.EndpointFilter;
import com.spotify.nameless.api.NamelessClient;
import com.spotify.nameless.proto.Messages;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.AgentStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.nameless.proto.Messages.RegistryEntry;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NamelessRegistrationTest extends NamelessTestBase {

  // TODO (dano): there's two tests in here to keep them from running concurrently

  @Test
  public void masterTest() throws Exception {
    startMaster("-vvvv",
                "--no-log-setup",
                "--munin-port", "0",
                "--site", "localhost",
                "--http", "0.0.0.0:" + EXTERNAL_PORT,
                "--hm", masterEndpoint,
                "--zk", zookeeperEndpoint);

    final NamelessClient client = new NamelessClient(hermesClient("tcp://localhost:4999"));

    // Wait for the master to get registered with nameless
    List<RegistryEntry> entries = await(LONG_WAIT_MINUTES, MINUTES, new Callable<List<RegistryEntry>>() {
      @Override
      public List<RegistryEntry> call() throws Exception {
        List<RegistryEntry> entries = client.queryEndpoints(EndpointFilter.everything()).get();
        return entries.size() == 2 ? entries : null;
      }
    });

    boolean httpFound = false;
    boolean hermesFound = false;

    for (RegistryEntry entry : entries) {
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

  @Test
  public void containerTest() throws Exception {
    startDefaultMaster();

    final Client client = defaultClient();

    startDefaultAgent(TEST_AGENT, "--site=localhost");
    awaitAgentStatus(client, TEST_AGENT, UP, LONG_WAIT_MINUTES, MINUTES);

    ImmutableMap<String, PortMapping> portMapping = ImmutableMap.of(
        "PORT_NAME", PortMapping.of(INTERNAL_PORT, EXTERNAL_PORT));

    final String serviceName = "SERVICE";
    final String serviceProto = "PROTO";

    ImmutableMap<ServiceEndpoint, ServicePorts> registration = ImmutableMap.of(
        ServiceEndpoint.of(serviceName, serviceProto), ServicePorts.of("PORT_NAME"));

    final JobId jobId = createJob(JOB_NAME, JOB_VERSION, "busybox", DO_NOTHING_COMMAND,
                                  EMPTY_ENV, portMapping, registration);

    deployJob(jobId, TEST_AGENT);
    awaitJobState(client, TEST_AGENT, jobId, RUNNING, LONG_WAIT_MINUTES, MINUTES);

    final NamelessClient nameless = new NamelessClient(hermesClient("tcp://localhost:4999"));
    final EndpointFilter filter = EndpointFilter.newBuilder()
        .port(EXTERNAL_PORT)
        .protocol(serviceProto)
        .service(serviceName)
        .build();

    // Wait for the container to get registered with nameless
    final List<RegistryEntry> entries = await(LONG_WAIT_MINUTES, MINUTES, new Callable<List<RegistryEntry>>() {
      @Override
      public List<RegistryEntry> call() throws Exception {
        List<RegistryEntry> entries = nameless.queryEndpoints(filter).get();
        return entries.size() == 1 ? entries : null;
      }
    });

    assertTrue(entries.size() == 1);
    final RegistryEntry entry = entries.get(0);
    final Messages.Endpoint endpoint = entry.getEndpoint();
    assertEquals("wrong service", serviceName, endpoint.getService());
    assertEquals("wrong protocol", serviceProto, endpoint.getProtocol());
    assertEquals("wrong port", endpoint.getPort(), EXTERNAL_PORT);
  }
}
