/*-
 * -\-\-
 * Helios System Tests
 * --
 * Copyright (C) 2017 Spotify AB
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
import static org.junit.Assume.assumeFalse;

import com.spotify.helios.Polling;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.concurrent.Callable;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RamdiskTest extends SystemTestBase {

  private HeliosClient client;
  private Job job;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Doesn't work on CircleCI because they don't support creating tmpfs using docker.
    // https://discuss.circleci.com/t/docker-tmpfs-support/10416
    assumeFalse(isCircleCi());
  }

  @Before
  public void setup() throws Exception {
    startDefaultMaster();

    client = defaultClient();
    startDefaultAgent(testHost());

    job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .addRamdisk("/much-volatile", "rw,noexec,nosuid,size=1")
        .setCommand(asList("sh", "-c", "nc -p 4711 -lle sh -c \"df | grep much-volatile\""))
        .addPort("df", PortMapping.of(4711))
        .setCreatingUser(TEST_USER)
        .build();
  }

  @Test
  public void testClient() throws Exception {
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());
    assertRamdisk(job.getId());
  }

  @Test
  public void testCli() throws Exception {
    final JobId jobId = createJob(job);
    assertRamdisk(jobId);
  }

  public void assertRamdisk(final JobId jobId) throws Exception {
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

    final Integer dfPort = taskStatus.getPorts().get("df").getExternalPort();

    assert dfPort != null;

    // If "/much-volatile" mount is present a line starting with tmpfs should be returned
    final String dfOutput = recvUtf8(dfPort, 5);
    assertEquals("tmpfs", dfOutput);
  }

  private String recvUtf8(final int port, final int numBytes) throws Exception {
    final byte[] bytes = recv(port, numBytes);
    return new String(bytes, UTF_8);
  }

  private byte[] recv(final int port, final int numBytes) throws Exception {
    checkArgument(numBytes > 0, "numBytes must be > 0");
    return Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<byte[]>() {
      @Override
      public byte[] call() {
        try (final Socket s = new Socket(DOCKER_HOST.address(), port)) {
          final byte[] bytes = new byte[numBytes];
          final InputStream is = s.getInputStream();
          final int first = is.read();
          // Check if the uml kernel slirp driver did an accept->close on us,
          // i.e. the actual listener is not up yet
          if (first == -1) {
            return null;
          }
          bytes[0] = (byte) first;
          for (int i = 1; i < numBytes; i++) {
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
