/*-
 * -\-\-
 * Helios System Tests
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.system;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.Job.EMPTY_ENV;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.serviceregistration.ServiceRegistration.Endpoint;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.spotify.helios.MockServiceRegistrarRegistry;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.serviceregistration.ServiceRegistration;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JobServiceRegistrationTest extends ServiceRegistrationTestBase {

  private final int externalPort = temporaryPorts.localPort("external");

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

    startDefaultAgent(testHost(), "--service-registry=" + registryAddress);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    final ImmutableMap<String, PortMapping> portMapping = ImmutableMap.of(
        "foo_port", PortMapping.of(4711, externalPort),
        "bar_port", PortMapping.of(4712));

    final ImmutableMap<ServiceEndpoint, ServicePorts> registration = ImmutableMap.of(
        ServiceEndpoint.of("foo_service", "foo_proto"), ServicePorts.of("foo_port"),
        ServiceEndpoint.of("bar_service", "bar_proto"), ServicePorts.of("bar_port"));

    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND,
        EMPTY_ENV, portMapping, registration);

    deployJob(jobId, testHost());
    awaitJobState(client, testHost(), jobId, RUNNING, LONG_WAIT_SECONDS, SECONDS);

    verify(registrar, timeout((int) SECONDS.toMillis(LONG_WAIT_SECONDS)))
        .register(registrationCaptor.capture());
    final ServiceRegistration serviceRegistration = registrationCaptor.getValue();

    final Map<String, Endpoint> registered = Maps.newHashMap();
    for (final Endpoint endpoint : serviceRegistration.getEndpoints()) {
      registered.put(endpoint.getName(), endpoint);
    }

    assertEquals("wrong service", "foo_service", registered.get("foo_service").getName());
    assertEquals("wrong protocol", "foo_proto", registered.get("foo_service").getProtocol());
    assertEquals("wrong port", externalPort, registered.get("foo_service").getPort());

    assertEquals("wrong service", "bar_service", registered.get("bar_service").getName());
    assertEquals("wrong protocol", "bar_proto", registered.get("bar_service").getProtocol());
    assertNotEquals("wrong port", externalPort, registered.get("bar_service").getPort());
  }
}
