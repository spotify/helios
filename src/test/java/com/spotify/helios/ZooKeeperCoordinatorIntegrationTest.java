package com.spotify.helios;

import com.google.common.collect.ImmutableList;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.spotify.helios.common.AgentDoesNotExistException;
import com.spotify.helios.common.AgentJobDoesNotExistException;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.JobDoesNotExistException;
import com.spotify.helios.common.ZooKeeperCurator;
import com.spotify.helios.common.coordination.CuratorInterface;
import com.spotify.helios.common.descriptors.AgentJob;
import com.spotify.helios.common.descriptors.JobDescriptor;
import com.spotify.helios.common.descriptors.JobGoal;
import com.spotify.helios.master.ZooKeeperCoordinator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

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
  private static final JobDescriptor JOB = JobDescriptor.newBuilder()
      .setCommand(ImmutableList.of(COMMAND))
      .setImage(IMAGE)
      .setName(JOB_NAME)
      .setVersion("VERSION")
      .build();

  private CuratorInterface curator;
  private ZooKeeperCoordinator coordinator;

  @Before
  public void setup() throws Exception {
    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);

    final CuratorFramework client = CuratorFrameworkFactory.newClient(
      zookeeperEndpoint, zooKeeperRetryPolicy);

    client.start();
    curator = new ZooKeeperCurator(client);

    coordinator = new ZooKeeperCoordinator(curator);
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
    } catch (AgentJobDoesNotExistException e) {
      assertTrue(true);
    } catch (Exception e) {
      fail("Should have thrown an AgentJobDoesNotExistException");
    }

    coordinator.addAgentJob(AGENT, AgentJob.newBuilder()
        .setGoal(JobGoal.START)
        .setJob(JOB.getId())
        .build());
    Map<String, JobDescriptor> jobsOnAgent = coordinator.getJobs();
    assertEquals(1, jobsOnAgent.size());
    JobDescriptor descriptor = jobsOnAgent.get(JOB.getId());
    assertEquals(JOB, descriptor);

    stopJob(coordinator, JOB); // should succeed this time!
    AgentJob jobCfg = coordinator.getAgentJob(AGENT, JOB.getId());
    assertEquals(JobGoal.STOP, jobCfg.getGoal());
  }

  private void stopJob(ZooKeeperCoordinator coordinator, JobDescriptor job) throws HeliosException {
    coordinator.updateAgentJob(AGENT, AgentJob.newBuilder()
        .setGoal(JobGoal.STOP)
        .setJob(job.getId())
        .build());
  }
}
