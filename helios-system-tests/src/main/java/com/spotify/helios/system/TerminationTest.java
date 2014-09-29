package com.spotify.helios.system;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Test;

import static com.spotify.docker.client.DockerClient.LogsParameter.STDERR;
import static com.spotify.docker.client.DockerClient.LogsParameter.STDOUT;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class TerminationTest extends SystemTestBase {

  @Test
  public void testNoIntOnExit() throws Exception {
    startDefaultMaster();

    final String host = testHost();
    startDefaultAgent(host);

    final HeliosClient client = defaultClient();

    awaitHostStatus(client, host, UP, LONG_WAIT_MINUTES, MINUTES);

    // Note: signal 2 is SIGINT
    final Job jobToInterrupt = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(asList("/bin/sh", "-c", "trap 2 handle; function handle { echo int }; "
                                            + "while true; do sleep 1; done"))
        .build();

    final JobId jobId = createJob(jobToInterrupt);
    deployJob(jobId, host);
    awaitTaskState(jobId, host, RUNNING);

    client.undeploy(jobId, host);

    final TaskStatus taskStatus = awaitTaskState(jobId, host, EXITED);

    final String log;
    try (final DefaultDockerClient dockerClient = new DefaultDockerClient(DOCKER_HOST.uri());
         LogStream logs = dockerClient.logs(taskStatus.getContainerId(), STDOUT, STDERR)) {
      log = logs.readFully();
    }

    // No message expected, since SIGINT should not be sent
    assertEquals("", log);
  }

  @Test
  public void testTermOnExit() throws Exception {
    startDefaultMaster();

    final String host = testHost();
    startDefaultAgent(host);

    final HeliosClient client = defaultClient();

    awaitHostStatus(client, host, UP, LONG_WAIT_MINUTES, MINUTES);

    // Note: signal 15 is SIGTERM
    final Job jobToInterrupt = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(asList("/bin/sh", "-c", "trap 15 handle; function handle { echo term }; "
                                            + "while true; do sleep 1; done"))
        .build();

    final JobId jobId = createJob(jobToInterrupt);
    deployJob(jobId, host);
    awaitTaskState(jobId, host, RUNNING);

    client.undeploy(jobId, host);

    final TaskStatus taskStatus = awaitTaskState(jobId, host, EXITED);

    final String log;
    try (final DefaultDockerClient dockerClient = new DefaultDockerClient(DOCKER_HOST.uri());
         LogStream logs = dockerClient.logs(taskStatus.getContainerId(), STDOUT, STDERR)) {
      log = logs.readFully();
    }

    // Message expected, because the SIGTERM handler in the script should have run
    assertEquals("term\n", log);
  }

}
