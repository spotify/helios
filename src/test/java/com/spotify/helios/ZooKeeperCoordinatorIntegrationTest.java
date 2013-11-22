package com.spotify.helios;

import com.google.common.collect.ImmutableList;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.spotify.helios.common.AgentDoesNotExistException;
import com.spotify.helios.common.DefaultZooKeeperClient;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.JobDoesNotExistException;
import com.spotify.helios.common.JobNotDeployedException;
import com.spotify.helios.common.JobStillInUseException;
import com.spotify.helios.common.coordination.ZooKeeperClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.master.ZooKeeperMasterModel;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
  private ZooKeeperMasterModel model;

  @Before
  public void setup() throws Exception {
    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);

    final CuratorFramework client = CuratorFrameworkFactory.newClient(
        zookeeperEndpoint, zooKeeperRetryPolicy);

    client.start();
    curator = new DefaultZooKeeperClient(client);

    model = new ZooKeeperMasterModel(curator);
  }

  @Test
  public void testAgentAddRemoveList() throws Exception {
    final String secondAgent = "SECOND";

    assertThat(model.getAgents(), empty());

    model.addAgent(AGENT);
    assertThat(model.getAgents(), contains(AGENT));

    model.addAgent(secondAgent);
    assertThat(model.getAgents(), contains(AGENT, secondAgent));

    model.removeAgent(AGENT);
    assertThat(model.getAgents(), contains(secondAgent));
  }

  @Test
  public void testJobAddGet() throws Exception {
    assertThat(model.getJobs().entrySet(), empty());
    model.addJob(JOB);

    assertEquals(model.getJobs().get(JOB_ID), JOB);
    assertEquals(model.getJob(JOB_ID), JOB);

    final Job secondJob = Job.newBuilder()
        .setCommand(ImmutableList.of(COMMAND))
        .setImage(IMAGE)
        .setName(JOB_NAME)
        .setVersion("SECOND")
        .build();

    model.addJob(secondJob);
    assertEquals(model.getJob(secondJob.getId()), secondJob);
    assertEquals(2, model.getJobs().size());
  }


  @Test
  public void testJobRemove() throws Exception {
    model.addJob(JOB);
    model.addAgent(AGENT);

    model.deployJob(AGENT,
                    Deployment.newBuilder().setGoal(Goal.START).setJobId(JOB_ID).build());
    try {
      model.removeJob(JOB_ID);
      fail("should have thrown an exception");
    } catch (JobStillInUseException e) {
      assertTrue(true);
    }

    model.undeployJob(AGENT, JOB_ID);
    assertNotNull(model.getJobs().get(JOB_ID));
    model.removeJob(JOB_ID); // should succeed
    assertNull(model.getJobs().get(JOB_ID));
  }

  @Test
  public void testDeploy() throws Exception {
    try {
      model.deployJob(AGENT,
                      Deployment.newBuilder()
                          .setGoal(Goal.START)
                          .setJobId(JOB_ID)
                          .build());
      fail("should throw");
    } catch (JobDoesNotExistException | AgentDoesNotExistException e) {
      assertTrue(true);
    }

    model.addJob(JOB);
    try {
      model.deployJob(AGENT,
                      Deployment.newBuilder().setGoal(Goal.START).setJobId(JOB_ID).build());
      fail("should throw");
    } catch (AgentDoesNotExistException e) {
      assertTrue(true);
    }

    model.addAgent(AGENT);

    model.deployJob(AGENT,
                    Deployment.newBuilder().setGoal(Goal.START).setJobId(JOB_ID).build());

    model.undeployJob(AGENT, JOB_ID);
    model.removeJob(JOB_ID);

    try {
      model.deployJob(AGENT,
                      Deployment.newBuilder().setGoal(Goal.START).setJobId(JOB_ID).build());
      fail("should throw");
    } catch (JobDoesNotExistException e) {
      assertTrue(true);
    }
  }

  @Test
  public void testAgentRemove() throws Exception {
    model.addAgent(AGENT);
    List<String> agents1 = model.getAgents();
    assertThat(agents1, hasItem(AGENT));

    model.removeAgent(AGENT);
    List<String> agents2 = model.getAgents();
    assertEquals(0, agents2.size());
  }

  @Test
  public void testUpdateDeploy() throws Exception {
    try {
      stopJob(model, JOB);
      fail("should have thrown JobDoesNotExistException");
    } catch (JobDoesNotExistException e) {
      assertTrue(true);
    } catch (Exception e) {
      fail("Should have thrown an JobDoesNotExistException, got " + e.getClass());
    }

    model.addJob(JOB);
    try {
      stopJob(model, JOB);
      fail("should have thrown exception");
    } catch (AgentDoesNotExistException e) {
      assertTrue(true);
    } catch (Exception e) {
      fail("Should have thrown an AgentDoesNotExistException");
    }

    model.addAgent(AGENT);
    List<String> agents = model.getAgents();
    assertThat(agents, hasItem(AGENT));

    try {
      stopJob(model, JOB);
      fail("should have thrown exception");
    } catch (JobNotDeployedException e) {
      assertTrue(true);
    } catch (Exception e) {
      fail("Should have thrown an JobNotDeployedException");
    }

    model.deployJob(AGENT, Deployment.newBuilder()
        .setGoal(Goal.START)
        .setJobId(JOB.getId())
        .build());
    Map<JobId, Job> jobsOnAgent = model.getJobs();
    assertEquals(1, jobsOnAgent.size());
    Job descriptor = jobsOnAgent.get(JOB.getId());
    assertEquals(JOB, descriptor);

    stopJob(model, JOB); // should succeed this time!
    Deployment jobCfg = model.getDeployment(AGENT, JOB.getId());
    assertEquals(Goal.STOP, jobCfg.getGoal());
  }

  private void stopJob(ZooKeeperMasterModel model, Job job) throws HeliosException {
    model.updateDeployment(AGENT, Deployment.newBuilder()
        .setGoal(Goal.STOP)
        .setJobId(job.getId())
        .build());
  }
}
