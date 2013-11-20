package com.spotify.helios;

import com.google.common.collect.ImmutableList;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.spotify.helios.common.AgentDoesNotExistException;
import com.spotify.helios.common.JobNotDeployedException;
import com.spotify.helios.common.DefaultZooKeeperClient;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.JobDoesNotExistException;
import com.spotify.helios.common.JobStillInUseException;
import com.spotify.helios.common.coordination.ZooKeeperClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.master.ZooKeeperCoordinator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class ZooKeeperCoordinatorIntegrationTest extends ZooKeeperTestBase {
  private static final String IMAGE = "IMAGE";
  private static final String COMMAND = "COMMAND";
  private static final String JOB_NAME = "JOB_NAME";
  private static final String AGENT = "AGENT";
  private static final Job JOB = Job.newBuilder()
      .setCommand(ImmutableList.of(COMMAND))
      .setImage(IMAGE)
      .setName(JOB_NAME)
      .setVersion("VERSION")
      .build();
  private static final JobId JOB_ID = JOB.getId();

  private ZooKeeperClient curator;
  private ZooKeeperCoordinator coordinator;

  @Before
  public void setup() throws Exception {
    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);

    final CuratorFramework client = CuratorFrameworkFactory.newClient(
      zookeeperEndpoint, zooKeeperRetryPolicy);

    client.start();
    curator = new DefaultZooKeeperClient(client);

    coordinator = new ZooKeeperCoordinator(curator);
  }

  @Test
  public void testAgentAddRemoveList() throws Exception {
    final String secondAgent = "SECOND";

    assertThat(coordinator.getAgents(), empty());

    coordinator.addAgent(AGENT);
    assertThat(coordinator.getAgents(), contains(AGENT));

    coordinator.addAgent(secondAgent);
    assertThat(coordinator.getAgents(), contains(AGENT, secondAgent));

    coordinator.removeAgent(AGENT);
    assertThat(coordinator.getAgents(), contains(secondAgent));
  }

  @Test
  public void testJobAddGet() throws Exception {
    assertThat(coordinator.getJobs().entrySet(), empty());
    coordinator.addJob(JOB);

    assertEquals(coordinator.getJobs().get(JOB_ID), JOB);
    assertEquals(coordinator.getJob(JOB_ID), JOB);

    final Job secondJob = Job.newBuilder()
        .setCommand(ImmutableList.of(COMMAND))
        .setImage(IMAGE)
        .setName(JOB_NAME)
        .setVersion("SECOND")
        .build();

    coordinator.addJob(secondJob);
    assertEquals(coordinator.getJob(secondJob.getId()), secondJob);
    assertEquals(2, coordinator.getJobs().size());
  }


  @Test
  public void testJobRemove() throws Exception {
    coordinator.addJob(JOB);
    coordinator.addAgent(AGENT);

    coordinator.deployJob(AGENT,
                          Deployment.newBuilder().setGoal(Goal.START).setJobId(JOB_ID).build());
    try {
      coordinator.removeJob(JOB_ID);
      fail("should have thrown an exception");
    } catch (JobStillInUseException e) {
      assertTrue(true);
    }

    coordinator.undeployJob(AGENT, JOB_ID);
    assertNotNull(coordinator.getJobs().get(JOB_ID));
    coordinator.removeJob(JOB_ID); // should succeed
    assertNull(coordinator.getJobs().get(JOB_ID));
  }

  @Test
  public void testDeploy() throws Exception {
    try {
      coordinator.deployJob(AGENT,
                            Deployment.newBuilder()
                                .setGoal(Goal.START)
                                .setJobId(JOB_ID)
                                .build());
      fail("should throw");
    } catch (JobDoesNotExistException | AgentDoesNotExistException e) {
      assertTrue(true);
    }

    coordinator.addJob(JOB);
    try {
      coordinator.deployJob(AGENT,
                            Deployment.newBuilder().setGoal(Goal.START).setJobId(JOB_ID).build());
      fail("should throw");
    } catch (AgentDoesNotExistException e) {
      assertTrue(true);
    }

    coordinator.addAgent(AGENT);

    coordinator.deployJob(AGENT,
                          Deployment.newBuilder().setGoal(Goal.START).setJobId(JOB_ID).build());

    coordinator.undeployJob(AGENT, JOB_ID);
    coordinator.removeJob(JOB_ID);

    try {
      coordinator.deployJob(AGENT,
                            Deployment.newBuilder().setGoal(Goal.START).setJobId(JOB_ID).build());
      fail("should throw");
    } catch (JobDoesNotExistException e) {
      assertTrue(true);
    }
  }

  @Test
  public void testAgentRemove() throws Exception {
    coordinator.addAgent(AGENT);
    List<String> agents1 = coordinator.getAgents();
    assertThat(agents1, hasItem(AGENT));

    coordinator.removeAgent(AGENT);
    List<String> agents2 = coordinator.getAgents();
    assertEquals(0, agents2.size());
  }

  @Test
  public void testUpdateDeploy() throws Exception {
    try {
      stopJob(coordinator, JOB);
      fail("should have thrown JobDoesNotExistException");
    } catch (JobDoesNotExistException e) {
      assertTrue(true);
    } catch (Exception e) {
      fail("Should have thrown an JobDoesNotExistException, got " + e.getClass());
    }

    coordinator.addJob(JOB);
    try {
      stopJob(coordinator, JOB);
      fail("should have thrown exception");
    } catch (AgentDoesNotExistException e) {
      assertTrue(true);
    } catch (Exception e) {
      fail("Should have thrown an AgentDoesNotExistException");
    }

    coordinator.addAgent(AGENT);
    List<String> agents = coordinator.getAgents();
    assertThat(agents, hasItem(AGENT));

    try {
      stopJob(coordinator, JOB);
      fail("should have thrown exception");
    } catch (JobNotDeployedException e) {
      assertTrue(true);
    } catch (Exception e) {
      fail("Should have thrown an JobNotDeployedException");
    }

    coordinator.deployJob(AGENT, Deployment.newBuilder()
        .setGoal(Goal.START)
        .setJobId(JOB.getId())
        .build());
    Map<JobId, Job> jobsOnAgent = coordinator.getJobs();
    assertEquals(1, jobsOnAgent.size());
    Job descriptor = jobsOnAgent.get(JOB.getId());
    assertEquals(JOB, descriptor);

    stopJob(coordinator, JOB); // should succeed this time!
    Deployment jobCfg = coordinator.getDeployment(AGENT, JOB.getId());
    assertEquals(Goal.STOP, jobCfg.getGoal());
  }

  private void stopJob(ZooKeeperCoordinator coordinator, Job job) throws HeliosException {
    coordinator.updateDeployment(AGENT, Deployment.newBuilder()
        .setGoal(Goal.STOP)
        .setJobId(job.getId())
        .build());
  }
}
