/**
* Copyright (C) 2014 Spotify AB
*/

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.MockServiceRegistrarRegistry;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.serviceregistration.ServiceRegistration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class JobServiceRegistrationTest extends ServiceRegistrationTestBase {
  private final int EXTERNAL_PORT = TEMPORARY_PORTS.localPort("external");

  @Mock
  public ServiceRegistrar registrar;

  @Captor
  public ArgumentCaptor<ServiceRegistration> registrationCaptor;

  final String registryAddress = uniqueRegistryAddress();

  @Before
  public void setup() {
    MockServiceRegistrarRegistry.set(registryAddress, registrar);
  }

  @After
  public void teardown() {
    MockServiceRegistrarRegistry.remove(registryAddress);
  }

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final HeliosClient client = defaultClient();

    startDefaultAgent(getTestHost(), "--service-registry=" + registryAddress);
    awaitHostStatus(client, getTestHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    final ImmutableMap<String, PortMapping> portMapping = ImmutableMap.of(
        "PORT_NAME", PortMapping.of(INTERNAL_PORT, EXTERNAL_PORT));

    final String serviceName = "SERVICE";
    final String serviceProto = "PROTO";

    final ImmutableMap<ServiceEndpoint, ServicePorts> registration = ImmutableMap.of(
        ServiceEndpoint.of(serviceName, serviceProto), ServicePorts.of("PORT_NAME"));

    final JobId jobId = createJob(JOB_NAME, JOB_VERSION, "busybox", DO_NOTHING_COMMAND,
                                  EMPTY_ENV, portMapping, registration);

    deployJob(jobId, getTestHost());
    awaitJobState(client, getTestHost(), jobId, RUNNING, LONG_WAIT_MINUTES, MINUTES);

    verify(registrar, timeout((int) MINUTES.toMillis(LONG_WAIT_MINUTES))).register(registrationCaptor.capture());
    final ServiceRegistration serviceRegistration = registrationCaptor.getValue();

    final ServiceRegistration.Endpoint endpoint = getOnlyElement(serviceRegistration.getEndpoints());

    assertEquals("wrong service", serviceName, endpoint.getName());
    assertEquals("wrong protocol", serviceProto, endpoint.getProtocol());
    assertEquals("wrong port", endpoint.getPort(), EXTERNAL_PORT);
  }
}
