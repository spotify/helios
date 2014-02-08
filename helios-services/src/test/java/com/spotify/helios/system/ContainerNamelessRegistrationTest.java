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

import static com.spotify.helios.common.descriptors.AgentStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ContainerNamelessRegistrationTest extends NamelessTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final Client client = defaultClient();

    startDefaultAgent(TEST_AGENT, "--site=localhost");
    awaitAgentStatus(client, TEST_AGENT, UP, WAIT_TIMEOUT_SECONDS, SECONDS);

    ImmutableMap<String, PortMapping> portMapping = ImmutableMap.of(
        "PORT_NAME", PortMapping.of(INTERNAL_PORT, EXTERNAL_PORT));

    final String serviceName = "SERVICE";
    final String serviceProto = "PROTO";

    ImmutableMap<ServiceEndpoint, ServicePorts> registration = ImmutableMap.of(
        ServiceEndpoint.of(serviceName, serviceProto), ServicePorts.of("PORT_NAME"));

    final JobId jobId = createJob(JOB_NAME, JOB_VERSION, "busybox", DO_NOTHING_COMMAND,
                                  EMPTY_ENV, portMapping, registration);

    deployJob(jobId, TEST_AGENT);
    awaitJobState(client, TEST_AGENT, jobId, RUNNING, WAIT_TIMEOUT_SECONDS, SECONDS);
    // Give it some time for the registration to register.
    Thread.sleep(1000);
    final NamelessClient nameless = new NamelessClient(hermesClient("tcp://localhost:4999"));
    final EndpointFilter filter = EndpointFilter.newBuilder()
        .port(EXTERNAL_PORT)
        .protocol(serviceProto)
        .service(serviceName)
        .build();

    final List<Messages.RegistryEntry> entries = nameless.queryEndpoints(filter).get();
    assertTrue(entries.size() == 1);
    final Messages.RegistryEntry entry = entries.get(0);
    final Messages.Endpoint endpoint = entry.getEndpoint();
    assertEquals("wrong service", serviceName, endpoint.getService());
    assertEquals("wrong protocol", serviceProto, endpoint.getProtocol());
    assertEquals("wrong port", endpoint.getPort(), EXTERNAL_PORT);
  }

}
