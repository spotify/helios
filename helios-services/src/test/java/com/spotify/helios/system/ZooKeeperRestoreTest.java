/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.spotify.helios.ZooKeeperStandaloneServerManager;
import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.servicescommon.coordination.Paths;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class ZooKeeperRestoreTest extends SystemTestBase {

  private static final Job FOO = Job.newBuilder()
      .setName(JOB_NAME)
      .setVersion(JOB_VERSION)
      .setImage("busybox")
      .setCommand(DO_NOTHING_COMMAND)
      .build();

  private final ZooKeeperStandaloneServerManager zkc = new ZooKeeperStandaloneServerManager();

  private HeliosClient client;
  private Path backupDir;

  @Override
  protected ZooKeeperTestManager zooKeeperTestManager() {
    return zkc;
  }

  @Before
  public void setup() throws Exception {
    backupDir = Files.createTempDirectory("helios-zk-updating-persistent-dir-test-backup-");

    startDefaultMaster();
    client = defaultClient();

    final CreateJobResponse created = client.createJob(FOO).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());
  }

  @After
  public void teardown() throws Exception {
    FileUtils.deleteQuietly(backupDir.toFile());
  }

  @Test
  public void verifyAgentReRegistersAfterRestore() throws Exception {
    // Back up zk
    zkc.backup(backupDir);

    // Start agent
    startDefaultAgent(TEST_HOST);
    awaitHostStatus(client, TEST_HOST, UP, LONG_WAIT_MINUTES, MINUTES);

    // Restore zk, erasing task state
    zkc.stop();
    zkc.restore(backupDir);
    zkc.start();

    // Wait for agent to reregister
    awaitHostRegistered(client, TEST_HOST, LONG_WAIT_MINUTES, MINUTES);
    awaitHostStatus(client, TEST_HOST, UP, LONG_WAIT_MINUTES, MINUTES);
  }

  @Test
  public void verifyAgentPushesTaskStateAfterRestore() throws Exception {

    // Start agent once to have it register
    final AgentMain agent1 = startDefaultAgent(TEST_HOST);
    awaitHostStatus(client, TEST_HOST, UP, LONG_WAIT_MINUTES, MINUTES);
    agent1.stopAsync().awaitTerminated();

    // Deploy job
    final Deployment deployment = Deployment.of(FOO.getId(), START);
    final JobDeployResponse deployed = client.deploy(deployment, TEST_HOST).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Back up zk
    zkc.backup(backupDir);

    // Start agent
    startDefaultAgent(TEST_HOST);
    awaitHostStatus(client, TEST_HOST, UP, LONG_WAIT_MINUTES, MINUTES);

    // Wait for agent to indicate that job is running
    awaitJobState(client, TEST_HOST, FOO.getId(), RUNNING, LONG_WAIT_MINUTES, MINUTES);

    // Restore zk, erasing task state
    zkc.stop();
    zkc.restore(backupDir);
    zkc.start();

    // Wait for agent to again indicate that job is running
    awaitJobState(client, TEST_HOST, FOO.getId(), RUNNING, LONG_WAIT_MINUTES, MINUTES);

    // Remove task status
    zkc.curator().delete().forPath(Paths.statusHostJob(TEST_HOST, FOO.getId()));
  }
}
