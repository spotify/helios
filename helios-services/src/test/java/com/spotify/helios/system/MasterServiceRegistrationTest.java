/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.spotify.helios.MockServiceRegistrarRegistry;
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
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MasterServiceRegistrationTest extends ServiceRegistrationTestBase {

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
    startMaster("-vvvv",
                "--no-log-setup",
                "--http", masterEndpoint,
                "--admin=" + masterAdminPort,
                "--zk", zk.connectString(),
                "--service-registry=" + registryAddress);

    verify(registrar, timeout((int) MINUTES.toMillis(LONG_WAIT_MINUTES))).register(registrationCaptor.capture());
    final ServiceRegistration registration = registrationCaptor.getValue();

    final ServiceRegistration.Endpoint endpoint = getOnlyElement(registration.getEndpoints());
    assertEquals("http", endpoint.getProtocol());
    assertEquals("helios", endpoint.getName());
    assertEquals(masterPort, endpoint.getPort());
  }
}
