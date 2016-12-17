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

import static com.google.common.base.Preconditions.checkArgument;
import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

import com.spotify.helios.Polling;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.concurrent.Callable;

public class VolumeTest extends SystemTestBase {

  private HeliosClient client;
  private Job job;

  @Before
  public void setup() throws Exception {
    startDefaultMaster();

    client = defaultClient();
    startDefaultAgent(testHost());

    job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .addVolume("/volume")
        .addVolume("/hostname", "/etc/hostname")
        .setCommand(asList("sh", "-c", "echo foo > /volume/bar; " +
                                       "nc -p 4711 -le dd if=/volume/bar;" +
                                       "nc -p 4712 -lle dd if=/hostname"))
        .addPort("bar", PortMapping.of(4711))
        .addPort("hostname", PortMapping.of(4712))
        .setCreatingUser(TEST_USER)
        .build();
  }

  @Test
  public void testClient() throws Exception {
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());
    assertVolumes(job.getId());
  }

  @Test
  public void testCli() throws Exception {
    final JobId jobId = createJob(job);
    assertVolumes(jobId);
  }

  public void assertVolumes(final JobId jobId) throws Exception {
    // Wait for agent to come up
    awaitHostRegistered(client, testHost(), LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, testHost()).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    final TaskStatus taskStatus = awaitJobState(
        client, testHost(), jobId, RUNNING, LONG_WAIT_SECONDS, SECONDS);
    assertJobEquals(job, taskStatus.getJob());

    final Integer barPort = taskStatus.getPorts().get("bar").getExternalPort();
    final Integer hostnamePort = taskStatus.getPorts().get("hostname").getExternalPort();

    assert barPort != null;
    assert hostnamePort != null;

    // Read "foo" from /volume/bar
    final String foo = recvUtf8(barPort, 3);
    assertEquals("foo", foo);

    // Read hostname from /hostname
    final String hostname = getNewDockerClient().info().name();
    final String mountedHostname = recvUtf8(hostnamePort, hostname.length());
    assertEquals(hostname, mountedHostname);
  }

  private String recvUtf8(final int port, final int n) throws Exception {
    final byte[] bytes = recv(port, n);
    return new String(bytes, UTF_8);
  }

  private byte[] recv(final int port, final int n) throws Exception {
    checkArgument(n > 0, "n must be > 0");
    return Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<byte[]>() {
      @Override
      public byte[] call() {
        try (final Socket s = new Socket(DOCKER_HOST.address(), port)) {
          final byte[] bytes = new byte[n];
          final InputStream is = s.getInputStream();
          final int first = is.read();
          // Check if the uml kernel slirp driver did an accept->close on us,
          // i.e. the actual listener is not up yet
          if (first == -1) {
            return null;
          }
          bytes[0] = (byte) first;
          for (int i = 1; i < n; i++) {
            bytes[i] = (byte) is.read();
          }
          return bytes;
        } catch (IOException e) {
          return null;
        }
      }
    });
  }
}
