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
import static com.spotify.helios.common.descriptors.TaskStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.HEALTHCHECKING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.serviceregistration.ServiceRegistration.Endpoint;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.ExecCreation;
import com.spotify.docker.client.messages.Version;
import com.spotify.helios.MockServiceRegistrarRegistry;
import com.spotify.helios.Polling;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.ExecHealthCheck;
import com.spotify.helios.common.descriptors.HealthCheck;
import com.spotify.helios.common.descriptors.HttpHealthCheck;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.common.descriptors.TcpHealthCheck;
import com.spotify.helios.common.protocol.TaskStatusEvents;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.serviceregistration.ServiceRegistration;
import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HealthCheckTest extends ServiceRegistrationTestBase {

  @Mock
  public ServiceRegistrar registrar;

  @Captor
  public ArgumentCaptor<ServiceRegistration> registrationCaptor;

  private final String registryAddress = uniqueRegistryAddress();

  @Before
  public void setup() {
    MockServiceRegistrarRegistry.set(registryAddress, registrar);
  }

  @After
  public void teardown() {
    MockServiceRegistrarRegistry.remove(registryAddress);
  }

  @Test
  public void testTcp() throws Exception {
    startDefaultMaster();

    final HeliosClient client = defaultClient();

    startDefaultAgent(testHost(), "--service-registry=" + registryAddress);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    final HealthCheck healthCheck = TcpHealthCheck.of("health");

    final Job job = pokeJob(healthCheck);

    assertContainerRegistersAfterPoke(client, job);
  }

  private void assertContainerRegistersAfterPoke(final HeliosClient client, final Job job)
      throws Exception {
    final JobId jobId = createJob(job);
    deployJob(jobId, testHost());
    awaitTaskState(jobId, testHost(), HEALTHCHECKING);

    // wait a few seconds to see if the service gets registered
    Thread.sleep(3000);
    // shouldn't be registered, since we haven't poked it yet
    verify(registrar, never()).register(any(ServiceRegistration.class));

    pokeAndVerifyRegistration(client, jobId, LONG_WAIT_SECONDS);

    final ServiceRegistration serviceRegistration = registrationCaptor.getValue();

    final Map<String, Endpoint> registered = Maps.newHashMap();
    for (final Endpoint endpoint : serviceRegistration.getEndpoints()) {
      registered.put(endpoint.getName(), endpoint);
    }

    assertEquals("wrong service", "foo_service", registered.get("foo_service").getName());
    assertEquals("wrong protocol", "foo_proto", registered.get("foo_service").getProtocol());
  }

  @Test
  public void testHttp() throws Exception {
    startDefaultMaster();

    final HeliosClient client = defaultClient();

    startDefaultAgent(testHost(), "--service-registry=" + registryAddress);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    final HealthCheck healthCheck = HttpHealthCheck.of("http", "/");

    // start a container that listens on a poke port, and once poked runs a web server
    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(NGINX)
        .setCommand(asList("sh", "-c", "nc -l -p 4711 && nginx -g 'daemon off;'"))
        .addPort("poke", PortMapping.of(4711))
        .addPort("http", PortMapping.of(80))
        .addRegistration(ServiceEndpoint.of("foo_service", "foo_proto"), ServicePorts.of("http"))
        .setHealthCheck(healthCheck)
        .build();

    assertContainerRegistersAfterPoke(client, job);
  }

  @Test
  public void testExec() throws Exception {
    // CircleCI uses lxc which doesn't support exec - https://circleci.com/docs/docker/#docker-exec
    assumeFalse(isCircleCi());

    final DockerClient dockerClient = getNewDockerClient();
    assumeThat(dockerClient.version(), is(execCompatibleDockerVersion()));

    startDefaultMaster();

    final HeliosClient client = defaultClient();

    startDefaultAgent(testHost(), "--service-registry=" + registryAddress);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // check if "file" exists in the root directory as a healthcheck
    final HealthCheck healthCheck = ExecHealthCheck.of("test", "-e", "file");

    // start a container that listens on the service port
    final String portName = "service";
    final String serviceName = "foo_service";
    final String serviceProtocol = "foo_proto";
    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(asList("sh", "-c", "nc -l -p 4711"))
        .addPort(portName, PortMapping.of(4711))
        .addRegistration(ServiceEndpoint.of(serviceName, serviceProtocol),
            ServicePorts.of(portName))
        .setHealthCheck(healthCheck)
        .build();

    final JobId jobId = createJob(job);
    deployJob(jobId, testHost());
    final TaskStatus jobState = awaitTaskState(jobId, testHost(), HEALTHCHECKING);

    // wait a few seconds to see if the service gets registered
    Thread.sleep(3000);
    // shouldn't be registered, since we haven't created the file yet
    verify(registrar, never()).register(any(ServiceRegistration.class));

    // create the file in the container to make the healthcheck succeed
    final String[] makeFileCmd = new String[]{ "touch", "file" };
    final ExecCreation execCreation = dockerClient.execCreate(
        jobState.getContainerId(), makeFileCmd);
    final String execId = execCreation.id();
    dockerClient.execStart(execId);

    awaitTaskState(jobId, testHost(), RUNNING);
    dockerClient.close();

    verify(registrar, timeout((int) SECONDS.toMillis(LONG_WAIT_SECONDS)))
        .register(registrationCaptor.capture());
    final ServiceRegistration serviceRegistration = registrationCaptor.getValue();

    assertEquals(1, serviceRegistration.getEndpoints().size());
    final Endpoint registeredEndpoint = serviceRegistration.getEndpoints().get(0);

    assertEquals("wrong service", serviceName, registeredEndpoint.getName());
    assertEquals("wrong protocol", serviceProtocol, registeredEndpoint.getProtocol());
  }

  private static Matcher<Version> execCompatibleDockerVersion() {
    return new CustomTypeSafeMatcher<Version>("apiVersion >= 1.18") {
      @Override
      protected boolean matchesSafely(final Version version) {
        try {
          final Iterator<String> versionParts =
              Splitter.on('.').split(version.apiVersion()).iterator();
          final int apiVersionMajor = Integer.parseInt(versionParts.next());
          final int apiVersionMinor = Integer.parseInt(versionParts.next());
          return apiVersionMajor == 1 && apiVersionMinor >= 18;
        } catch (Exception e) {
          return false;
        }
      }
    };
  }

  @Test
  public void testContainerDiesDuringHealthcheck() throws Exception {
    startDefaultMaster();

    final HeliosClient client = defaultClient();

    startDefaultAgent(testHost(), "--service-registry=" + registryAddress);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    final HealthCheck healthCheck = TcpHealthCheck.of("health");
    final Job job = pokeJob(healthCheck);

    final JobId jobId = createJob(job);
    deployJob(jobId, testHost());
    awaitTaskState(jobId, testHost(), HEALTHCHECKING);

    // kill the underlying container
    final JobStatus jobStatus = getOrNull(client.jobStatus(jobId));
    final TaskStatus taskStatus = jobStatus.getTaskStatuses().get(testHost());
    getNewDockerClient().killContainer(taskStatus.getContainerId());

    // ensure the job is marked as failed
    final int timeout = WAIT_TIMEOUT_SECONDS;
    Polling.await(timeout, SECONDS, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final TaskStatusEvents jobHistory = getOrNull(client.jobHistory(jobId));
        for (final TaskStatusEvent event : jobHistory.getEvents()) {
          if (event.getStatus().getState() == FAILED) {
            return true;
          }
        }

        return null;
      }
    });

    // wait for the job to come back up and start healthchecking again
    awaitTaskState(jobId, testHost(), HEALTHCHECKING);

    pokeAndVerifyRegistration(client, jobId, timeout);
  }

  private void pokeAndVerifyRegistration(final HeliosClient client, final JobId jobId,
                                         final int timeout) throws Exception {
    Polling.await(timeout, SECONDS, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final JobStatus jobStatus = getOrNull(client.jobStatus(jobId));
        final TaskStatus taskStatus = jobStatus.getTaskStatuses().get(testHost());
        final PortMapping port = taskStatus.getPorts().get("poke");

        assert port.getExternalPort() != null;
        if (poke(port.getExternalPort())) {
          return true;
        } else {
          return null;
        }
      }
    });

    awaitTaskState(jobId, testHost(), RUNNING);
    verify(registrar, timeout((int) SECONDS.toMillis(timeout)))
        .register(registrationCaptor.capture());
  }

  /**
   * Starts a container that listens on a poke port, and once poked listens on the healthcheck
   * port.
   */
  private Job pokeJob(final HealthCheck healthCheck) {

    return Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(ALPINE)
        .setCommand(asList("sh", "-c",
            "nc -l -p 4711 && nc -lk -p 4712 -e hostname"))
        .addPort("poke", PortMapping.of(4711))
        .addPort("health", PortMapping.of(4712))
        .addRegistration(ServiceEndpoint.of("foo_service", "foo_proto"), ServicePorts.of("health"))
        .setHealthCheck(healthCheck)
        .build();
  }

  private boolean poke(final int port) {
    try (Socket ignored = new Socket(DOCKER_HOST.address(), port)) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
