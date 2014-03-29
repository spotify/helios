/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.Polling;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.nameless.api.EndpointFilter;
import com.spotify.nameless.proto.Messages;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.nameless.proto.Messages.RegistryEntry;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NamelessContainerRegistrationTest extends NamelessTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final HeliosClient client = defaultClient();

    startDefaultAgent(TEST_HOST, "--nameless=" + namelessEndpoint);
    awaitHostStatus(client, TEST_HOST, UP, LONG_WAIT_MINUTES, MINUTES);

    final ImmutableMap<String, PortMapping> portMapping = ImmutableMap.of(
        "PORT_NAME", PortMapping.of(INTERNAL_PORT, EXTERNAL_PORT1));

    final String serviceName = "SERVICE";
    final String serviceProto = "PROTO";

    final ImmutableMap<ServiceEndpoint, ServicePorts> registration = ImmutableMap.of(
        ServiceEndpoint.of(serviceName, serviceProto), ServicePorts.of("PORT_NAME"));

    final JobId jobId = createJob(JOB_NAME, JOB_VERSION, "busybox", DO_NOTHING_COMMAND,
                                  EMPTY_ENV, portMapping, registration);

    deployJob(jobId, TEST_HOST);
    awaitJobState(client, TEST_HOST, jobId, RUNNING, LONG_WAIT_MINUTES, MINUTES);

    final EndpointFilter filter = EndpointFilter.newBuilder()
        .port(EXTERNAL_PORT1)
        .protocol(serviceProto)
        .service(serviceName)
        .build();

    // Wait for the container to get registered with nameless
    final List<RegistryEntry> entries = Polling.await(
        LONG_WAIT_MINUTES, MINUTES, new Callable<List<RegistryEntry>>() {
      @Override
      public List<RegistryEntry> call() throws Exception {
        List<RegistryEntry> entries = namelessClient.queryEndpoints(filter).get();
        return entries.size() == 1 ? entries : null;
      }
    });

    assertTrue(entries.size() == 1);
    final RegistryEntry entry = entries.get(0);
    final Messages.Endpoint endpoint = entry.getEndpoint();
    assertEquals("wrong service", serviceName, endpoint.getService());
    assertEquals("wrong protocol", serviceProto, endpoint.getProtocol());
    assertEquals("wrong port", endpoint.getPort(), EXTERNAL_PORT1);
  }
}
