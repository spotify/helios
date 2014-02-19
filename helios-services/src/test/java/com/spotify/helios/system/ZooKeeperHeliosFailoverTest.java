/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.spotify.helios.ZooKeeperClusterTestManager;
import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.common.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobStatus;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import org.junit.Before;
import org.junit.Test;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class ZooKeeperHeliosFailoverTest extends SystemTestBase {

  private static final Job FOO = Job.newBuilder()
      .setName("foo")
      .setVersion(JOB_VERSION)
      .setImage("busybox")
      .setCommand(DO_NOTHING_COMMAND)
      .build();

  private static final Job BAR = Job.newBuilder()
      .setName("bar")
      .setVersion(JOB_VERSION)
      .setImage("busybox")
      .setCommand(DO_NOTHING_COMMAND)
      .build();

  private final ZooKeeperClusterTestManager zkc = new ZooKeeperClusterTestManager();

  private HeliosClient client;

  @Override
  protected ZooKeeperTestManager zooKeeperTestManager() {
    return zkc;
  }

  @Before
  public void setup() throws Exception {
    assert false : "this test cannot be run with assertions enabled";
    startDefaultMaster();
    startDefaultAgent(TEST_HOST);
    client = defaultClient();
    awaitHostStatus(client, TEST_HOST, UP, LONG_WAIT_MINUTES, MINUTES);
  }

  @Test
  public void verifyCanDeployWithOnePeerDead() throws Exception {
    deploy(FOO);
    zkc.stopPeer(0);
    undeploy(FOO.getId());
    deploy(BAR);
  }

  @Test
  public void verifyCanDeployWithOneNodeDeadAfterOneNodeDataLoss() throws Exception {
    deploy(FOO);
    zkc.stopPeer(0);
    zkc.resetPeer(0);
    zkc.startPeer(0);
    Thread.sleep(5000);
    zkc.stopPeer(1);
    undeploy(FOO.getId());
    deploy(BAR);
  }

  private void deploy(final Job job) throws Exception {
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, TEST_HOST).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    awaitJobState(client, TEST_HOST, jobId, RUNNING, LONG_WAIT_MINUTES, MINUTES);
  }

  private void undeploy(final JobId jobId) throws Exception {
    // Check job status can be queried
    final JobStatus jobStatus = client.jobStatus(jobId).get();
    assertEquals(RUNNING, jobStatus.getTaskStatuses().get(TEST_HOST).getState());

    // Undeploy the job
    final JobUndeployResponse undeployed = client.undeploy(jobId, TEST_HOST).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployed.getStatus());

    // Wait for the task to disappear
    awaitTaskGone(client, TEST_HOST, jobId, LONG_WAIT_MINUTES, MINUTES);
  }

}
