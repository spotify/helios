/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import com.spotify.helios.ZooKeeperClusterTestManager;
import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.google.common.base.Optional.fromNullable;
import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.lang.System.getenv;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ClusterDeploymentTest extends SystemTestBase {

  public static final int HOSTS =
      Integer.valueOf(fromNullable(getenv("HELIOS_CLUSTER_DEPLOYMENT_TEST_HOSTS")).or("10"));

  private static final Job FOO = Job.newBuilder()
      .setName("foo")
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
    startDefaultMaster();
    client = defaultClient();
  }

  @Test
  public void verifyCanDeployOnSeveralHosts() throws Exception {
    final CreateJobResponse created = client.createJob(FOO).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    final List<AgentMain> agents = Lists.newArrayList();

    for (int i = 0; i < HOSTS; i++) {
      final AgentMain agent =
          startDefaultAgent(host(i), "--no-http", "--no-metrics");
      agents.add(agent);
    }

    for (final AgentMain agent : agents) {
      agent.awaitRunning();
    }

    for (int i = 0; i < HOSTS; i++) {
      awaitHostStatus(client, host(i), UP, 10, MINUTES);
    }

    for (int i = 0; i < HOSTS; i++) {
      deploy(FOO, host(i));
    }

    for (int i = 0; i < HOSTS; i++) {
      awaitJobState(client, host(i), FOO.getId(), RUNNING, 10, MINUTES);
    }

    for (int i = 0; i < HOSTS; i++) {
      undeploy(FOO.getId(), host(i));
    }

    for (int i = 0; i < HOSTS; i++) {
      awaitTaskGone(client, host(i), FOO.getId(), 10, MINUTES);
    }
  }

  private String host(final int i) {return TEST_HOST + i;}

  private void deploy(final Job job, final String host) throws Exception {
    Futures.addCallback(client.deploy(Deployment.of(job.getId(), START), host),
                        new FutureCallback<JobDeployResponse>() {
                          @Override
                          public void onSuccess(final JobDeployResponse result) {
                            assertEquals(JobDeployResponse.Status.OK, result.getStatus());
                          }

                          @Override
                          public void onFailure(final Throwable t) {
                            fail("deploy failed");
                          }
                        });
  }

  private void undeploy(final JobId jobId, final String host) throws Exception {

    Futures.addCallback(client.undeploy(jobId, host), new FutureCallback<JobUndeployResponse>() {
      @Override
      public void onSuccess(final JobUndeployResponse result) {
        assertEquals(JobUndeployResponse.Status.OK, result.getStatus());
      }

      @Override
      public void onFailure(final Throwable t) {
        fail("undeploy failed");
      }
    });
  }
}
