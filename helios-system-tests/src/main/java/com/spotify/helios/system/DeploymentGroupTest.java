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

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getLast;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.helios.Polling;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.DeploymentGroupStatusResponse;
import com.spotify.helios.common.protocol.HostDeregisterResponse;
import com.spotify.helios.common.protocol.RemoveDeploymentGroupResponse;
import com.spotify.helios.common.protocol.RollingUpdateResponse;
import com.spotify.helios.master.MasterMain;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class DeploymentGroupTest extends SystemTestBase {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String TEST_GROUP = "my_group";
  private static final String TEST_LABEL = "foo=bar";
  private static final String TOKEN = "foo-token";

  private MasterMain master;

  @Rule
  public final TestName testName = new TestName();

  @Before
  public void initialize() throws Exception {
    System.out.printf("- %s\n", testName.getMethodName());

    master = startDefaultMaster();

    // Wait for master to come up
    Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<String>() {
      @Override
      public String call() throws Exception {
        final String output = cli("masters");
        return output.contains(masterName()) ? output : null;
      }
    });
  }

  @Test
  public void testListDeploymentGroups() throws Exception {
    cli("create-deployment-group", "group2", "foo=bar");
    cli("create-deployment-group", "group1", "foo=bar");
    final String output = cli("list-deployment-groups", "--json");
    final List<String> deploymentGroups = OBJECT_MAPPER.readValue(
        output, new TypeReference<List<String>>() {});
    assertEquals(Arrays.asList("group1", "group2"), deploymentGroups);
  }

  @Test
  public void testRollingUpdate() throws Exception {
    final List<String> hosts = ImmutableList.of(
        "dc1-" + testHost() + "-a1.dc1.example.com",
        "dc1-" + testHost() + "-a2.dc1.example.com",
        "dc2-" + testHost() + "-a1.dc2.example.com",
        "dc2-" + testHost() + "-a3.dc2.example.com",
        "dc3-" + testHost() + "-a4.dc3.example.com"
    );

    // start agents
    for (final String host : hosts) {
      startDefaultAgent(host, "--labels", TEST_LABEL);
    }

    // Wait for agents to come up
    final HeliosClient client = defaultClient();
    for (final String host : hosts) {
      awaitHostStatus(client, host, UP, LONG_WAIT_SECONDS, SECONDS);
    }

    // create a deployment group and job
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);

    // TODO: fix this!
    // Wait to make sure the host-update has run
    Thread.sleep(1000);

    // trigger a rolling update
    cli("rolling-update", "--async", testJobNameAndVersion, TEST_GROUP);

    // ensure the job is running on all agents and the deployment group reaches DONE
    for (final String host : hosts) {
      awaitTaskState(jobId, host, TaskStatus.State.RUNNING);
    }

    final Deployment deployment =
        defaultClient().hostStatus(hosts.get(0)).get().getJobs().get(jobId);
    assertEquals(TEST_GROUP, deployment.getDeploymentGroupName());
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);

    // create a second job
    final String secondJobVersion = testJobVersion + "2";
    final String secondJobNameAndVersion = testJobNameAndVersion + "2";
    final JobId secondJobId = createJob(testJobName, secondJobVersion, BUSYBOX, IDLE_COMMAND);

    // trigger a rolling update to replace the first job with the second job
    final String output = cli("rolling-update", secondJobNameAndVersion, TEST_GROUP);

    // Check that the hosts in the output are ordered
    final List<String> lines = Lists.newArrayList(Splitter.on("\n").split(output));
    for (int i = 0; i < hosts.size(); i++) {
      assertThat(lines.get(i + 2), containsString(hosts.get(i)));
    }

    // ensure the second job rolled out fine
    for (final String host : hosts) {
      awaitTaskState(secondJobId, host, TaskStatus.State.RUNNING);
    }
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);
  }

  @Test
  public void testAgentAddedAfterRollingUpdateIsDeployed() throws Exception {
    startDefaultAgent(testHost(), "--labels", "foo=bar");

    cli("create-deployment-group", "--json", TEST_GROUP, "foo=bar");
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);

    cli("rolling-update", "--async", testJobNameAndVersion, TEST_GROUP);

    awaitTaskState(jobId, testHost(), TaskStatus.State.RUNNING);

    // Rollout should be complete and on its second iteration at this point.
    // Start another agent and wait for it to have the job deployed to it.
    startDefaultAgent(testHost() + "2", "--labels", "foo=bar");

    awaitTaskState(jobId, testHost() + "2", TaskStatus.State.RUNNING);
  }

  private void awaitUpWithLabels(final String host, final String... labelPairs)
      throws Exception {

    Preconditions.checkArgument(labelPairs.length % 2 == 0,
        "Must pass even number of pairs for labels");

    final Map<String, String> labels = new HashMap<>();
    for (int i = 0; i < labelPairs.length - 1; i += 2) {
      labels.put(labelPairs[i], labelPairs[i + 1]);
    }

    awaitHostStatusWithLabels(defaultClient(), host, HostStatus.Status.UP, labels);
  }

  private void awaitUndeployed(final String host, final JobId jobId) throws Exception {
    // Ensure the deployment is gone.
    final Deployment oldDeployment = defaultClient().deployment(host, jobId).get();
    assertThat(oldDeployment, is(nullValue()));

    // Wait for the task to disappear
    awaitTaskGone(defaultClient(), host, jobId, LONG_WAIT_SECONDS, SECONDS);
  }

  @Test
  public void testRemovingAgentTagUndeploysJob() throws Exception {
    final HeliosClient client = defaultClient();

    final String oldHost = testHost();
    final String deregisterHost = testHost() + "2";
    final String unchangedHost = testHost() + "3";
    final String newHost = testHost() + "4";
    final String anotherNewHost = testHost() + "5";

    @SuppressWarnings("VariableDeclarationUsageDistance")
    AgentMain oldAgent = startDefaultAgent(oldHost, "--labels", "foo=bar");
    awaitUpWithLabels(oldHost, "foo", "bar");

    final AgentMain deregisterAgent = startDefaultAgent(deregisterHost, "--labels", "foo=bar");
    awaitUpWithLabels(deregisterHost, "foo", "bar");

    startDefaultAgent(unchangedHost, "--labels", "foo=bar");
    awaitUpWithLabels(unchangedHost, "foo", "bar");

    cli("create-deployment-group", "--json", TEST_GROUP, "foo=bar");
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);

    cli("rolling-update", "--async", testJobNameAndVersion, TEST_GROUP);

    awaitTaskState(jobId, oldHost, TaskStatus.State.RUNNING);
    awaitTaskState(jobId, deregisterHost, TaskStatus.State.RUNNING);
    awaitTaskState(jobId, unchangedHost, TaskStatus.State.RUNNING);
    awaitDeploymentGroupStatus(client, TEST_GROUP, DeploymentGroupStatus.State.DONE);

    // Rollout should be complete and on its second iteration at this point.
    // Start another agent and wait for it to have the job deployed to it.
    startDefaultAgent(newHost, "--labels", "foo=bar");
    awaitUpWithLabels(newHost, "foo", "bar");
    awaitDeploymentGroupStatus(client, TEST_GROUP, DeploymentGroupStatus.State.ROLLING_OUT);
    awaitDeploymentGroupStatus(client, TEST_GROUP, DeploymentGroupStatus.State.DONE);
    awaitTaskState(jobId, newHost, TaskStatus.State.RUNNING);

    // Restart the old agent with labels that still match the deployment group
    // The job should not be undeployed.
    stopAgent(oldAgent);
    oldAgent = startDefaultAgent(oldHost, "--labels", "foo=bar", "another=label");
    awaitUpWithLabels(oldHost, "foo", "bar", "another", "label");
    awaitTaskState(jobId, oldHost, TaskStatus.State.RUNNING);

    // Restart the old agent with labels that do not match the deployment group.
    stopAgent(oldAgent);
    oldAgent = startDefaultAgent(oldHost, "--labels", "foo=notbar");
    awaitUpWithLabels(oldHost, "foo", "notbar");

    // ...which should trigger a rolling update
    awaitDeploymentGroupStatus(client, TEST_GROUP, DeploymentGroupStatus.State.ROLLING_OUT);

    // Start yet another agent in order to trigger another rolling update.
    startDefaultAgent(anotherNewHost, "--labels", "foo=bar");

    // Wait for the rolling update(s) to finish.
    awaitDeploymentGroupStatus(client, TEST_GROUP, DeploymentGroupStatus.State.ROLLING_OUT);
    awaitDeploymentGroupStatus(client, TEST_GROUP, DeploymentGroupStatus.State.DONE);

    // ...which should remove the job.
    awaitUndeployed(oldHost, jobId);

    // Restart the old agent with labels that match the deployment group (again)
    // The job should be deployed.
    stopAgent(oldAgent);
    startDefaultAgent(oldHost, "--labels", "foo=bar");
    awaitDeploymentGroupStatus(client, TEST_GROUP, DeploymentGroupStatus.State.ROLLING_OUT);
    awaitDeploymentGroupStatus(client, TEST_GROUP, DeploymentGroupStatus.State.DONE);
    awaitTaskState(jobId, oldHost, TaskStatus.State.RUNNING);

    // Deregister an agent
    stopAgent(deregisterAgent);
    final HostDeregisterResponse deregisterResponse = client.deregisterHost(deregisterHost).get();
    assertEquals(HostDeregisterResponse.Status.OK, deregisterResponse.getStatus());

    // Make sure we 'undeploy' from the now non-existent agent.
    awaitDeploymentGroupStatus(client, TEST_GROUP, DeploymentGroupStatus.State.ROLLING_OUT);
    awaitDeploymentGroupStatus(client, TEST_GROUP, DeploymentGroupStatus.State.DONE);
  }

  @Test
  public void testRollingUpdateGroupNotFound() throws Exception {
    cli("create-deployment-group", "--json", TEST_GROUP, "foo=bar", "baz=qux");
    cli("create", "my_job:2", "my_image");
    assertEquals(RollingUpdateResponse.Status.DEPLOYMENT_GROUP_NOT_FOUND,
        OBJECT_MAPPER.readValue(cli("rolling-update", "--json", "--async", "my_job:2",
            "oops"),
            RollingUpdateResponse.class).getStatus());
  }

  @Test
  public void testStatusNoRollingUpdate() throws Exception {
    cli("create-deployment-group", "--json", TEST_GROUP, "foo=bar", "baz=qux");
    assertEquals(DeploymentGroupStatusResponse.Status.IDLE,
        OBJECT_MAPPER.readValue(cli("deployment-group-status", "--json", TEST_GROUP),
            DeploymentGroupStatusResponse.class).getStatus());
  }

  @Test
  public void testRollingUpdateMigrate() throws Exception {
    final String host = testHost();
    startDefaultAgent(host, "--labels", TEST_LABEL);

    // Wait for agent to come up
    final HeliosClient client = defaultClient();
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Manually deploy a job on the host (i.e. a job not part of the deployment group)
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);
    deployJob(jobId, host);
    awaitTaskState(jobId, host, TaskStatus.State.RUNNING);

    // Create a deployment-group and trigger a migration rolling-update
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);
    cli("rolling-update", "--async", "--migrate", testJobNameAndVersion, TEST_GROUP);

    // Check that the deployment's deployment-group name eventually changes to TEST_GROUP
    // (should be null or empty before)
    final String jobDeploymentGroup = Polling.await(
        LONG_WAIT_SECONDS, SECONDS, new Callable<String>() {
          @Override
          public String call() throws Exception {
            final Deployment deployment =
                defaultClient().hostStatus(host).get().getJobs().get(jobId);
            if (deployment != null && !isNullOrEmpty(deployment.getDeploymentGroupName())) {
              return deployment.getDeploymentGroupName();
            } else {
              return null;
            }
          }
        });
    assertEquals(TEST_GROUP, jobDeploymentGroup);

    // rolling-update should succeed & job should be running
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);
    awaitTaskState(jobId, host, TaskStatus.State.RUNNING);
  }

  @Test
  public void testRollingUpdateMigrateWithToken() throws Exception {
    final String host = testHost();
    startDefaultAgent(host, "--labels", TEST_LABEL);

    // Wait for agent to come up
    final HeliosClient client = defaultClient();
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Manually deploy a job with a token on the host (i.e. a job not part of the deployment group)
    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setToken(TOKEN)
        .build();
    final JobId jobId = createJob(job);
    deployJob(jobId, host, TOKEN);
    awaitTaskState(jobId, host, TaskStatus.State.RUNNING);

    // Create a deployment-group and trigger a migration rolling-update
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);
    cli("rolling-update", "--async", "--migrate", "--token", TOKEN, testJobNameAndVersion,
        TEST_GROUP);

    // Check that the deployment's deployment-group name eventually changes to TEST_GROUP
    // (should be null or empty before)
    final String jobDeploymentGroup = Polling.await(
        LONG_WAIT_SECONDS, SECONDS, new Callable<String>() {
          @Override
          public String call() throws Exception {
            final Deployment deployment =
                defaultClient().hostStatus(host).get().getJobs().get(jobId);
            if (deployment != null && !isNullOrEmpty(deployment.getDeploymentGroupName())) {
              return deployment.getDeploymentGroupName();
            } else {
              return null;
            }
          }
        });
    assertEquals(TEST_GROUP, jobDeploymentGroup);

    // rolling-update should succeed & job should be running
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);
    awaitTaskState(jobId, host, TaskStatus.State.RUNNING);
  }

  @Test
  public void testRollingUpdateMigrateNothingToUndeploy() throws Exception {
    final String host = testHost();
    startDefaultAgent(host, "--labels", TEST_LABEL);

    // Wait for agent to come up
    final HeliosClient client = defaultClient();
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Manually deploy a job on the host
    final String manualJobVersion = "foo-" + testJobVersion;
    final JobId manualJobId = createJob(testJobName, manualJobVersion, BUSYBOX, IDLE_COMMAND);
    deployJob(manualJobId, host);
    awaitTaskState(manualJobId, host, TaskStatus.State.RUNNING);

    // create a deployment group and trigger a migration rolling-update -- with a different
    // job that the one deployed manually! The manually deployed job should remain running on the
    // host.
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);
    cli("rolling-update", "--async", "--migrate", testJobNameAndVersion, TEST_GROUP);

    // rolling-update should succeed & job should be running
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP,
        DeploymentGroupStatus.State.DONE);
    awaitTaskState(jobId, host, TaskStatus.State.RUNNING);

    final String jobDeploymentGroup = Polling.await(
        LONG_WAIT_SECONDS, SECONDS, new Callable<String>() {
          @Override
          public String call() throws Exception {
            final Deployment deployment =
                defaultClient().hostStatus(host).get().getJobs().get(jobId);
            if (deployment != null && !isNullOrEmpty(deployment.getDeploymentGroupName())) {
              return deployment.getDeploymentGroupName();
            } else {
              return null;
            }
          }
        });
    assertEquals(TEST_GROUP, jobDeploymentGroup);

    // Ensure that the manually deployed job is still there & running
    final Deployment manualDeployment =
        defaultClient().hostStatus(host).get().getJobs().get(manualJobId);
    assertNotNull(manualDeployment);
    assertEquals(Goal.START, manualDeployment.getGoal());
  }

  @Test
  public void testStopDeploymentGroup() throws Exception {
    cli("create-deployment-group", "--json", TEST_GROUP, "foo=bar", "baz=qux");
    cli("create", "my_job:2", "my_image");
    assertThat(cli("stop-deployment-group", TEST_GROUP),
        containsString("Deployment-group my_group stopped"));
    final DeploymentGroupStatusResponse status = Json.read(
        cli("deployment-group-status", "--json", TEST_GROUP), DeploymentGroupStatusResponse.class);
    assertEquals(DeploymentGroupStatusResponse.Status.FAILED, status.getStatus());
    assertEquals("Stopped by user", status.getError());
  }


  @Test
  public void testStopDeploymentGroupGroupNotFound() throws Exception {
    assertThat(cli("stop-deployment-group", TEST_GROUP),
        containsString("Deployment-group my_group not found"));
  }

  @Test
  public void testRollingUpdateCoordination() throws Exception {
    // stop the default master
    master.stopAsync().awaitTerminated();

    // start a bunch of masters and agents
    final Map<String, MasterMain> masters = startDefaultMasters(3);

    final Map<String, AgentMain> agents = Maps.newLinkedHashMap();
    for (int i = 0; i < 20; i++) {
      final String name = TEST_HOST + i;
      agents.put(name, startDefaultAgent(name, "--labels", TEST_LABEL));
    }

    // create a deployment group and start rolling out
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);
    cli("rolling-update", "--async", "--par", String.valueOf(agents.size()), testJobNameAndVersion,
        TEST_GROUP);

    // wait until the task is running on the final agent
    awaitTaskState(jobId, getLast(agents.keySet()), TaskStatus.State.RUNNING);

    // ensure that all masters were involved
    final Set<String> deployingMasters = Sets.newHashSet();
    final Map<String, HostStatus> hostStatuses = defaultClient().hostStatuses(
        Lists.newArrayList(agents.keySet())).get();
    for (final HostStatus status : hostStatuses.values()) {
      for (final Deployment deployment : status.getJobs().values()) {
        deployingMasters.add(deployment.getDeployerMaster());
      }
    }

    assertEquals(masters.size(), deployingMasters.size());
  }

  @Test
  public void testIdenticalRollouts() throws Exception {
    // This verifies that calling rolling-update on a failed deployment group will initiate a
    // new rollout and bring the group status out of the failed state. Previously calling
    // rolling-update on a failed deployment group would have no effect unless you changed the job
    // ID or a rollout option.

    // create the deployment group and job
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);
    createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);

    // trigger a rolling update
    cli("rolling-update", "--async", testJobNameAndVersion, TEST_GROUP);
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);

    // stop the deployment group to put it in a failed state
    cli("stop-deployment-group", TEST_GROUP);
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.FAILED);

    // trigger another rolling update with the same params as before and verify it reaches done
    cli("rolling-update", "--async", testJobNameAndVersion, TEST_GROUP);
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);
  }

  @Test
  public void testStoppedJob() throws Exception {
    final String host = testHost();
    startDefaultAgent(host, "--labels", TEST_LABEL);

    // Wait for agent to come up
    final HeliosClient client = defaultClient();
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Create the deployment group and two jobs
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);
    final JobId firstJobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);
    final String secondJobVersion = randomHexString();
    final String secondJobNameAndVersion = testJobName + ":" + secondJobVersion;
    final JobId secondJobId = createJob(testJobName, secondJobVersion, BUSYBOX, IDLE_COMMAND);

    // Trigger a rolling update of the first job
    cli("rolling-update", "--async", testJobNameAndVersion, TEST_GROUP);
    awaitTaskState(firstJobId, host, TaskStatus.State.RUNNING);
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);

    // Stop the job
    cli("stop", testJobNameAndVersion, host);
    awaitTaskState(firstJobId, host, TaskStatus.State.STOPPED);

    // Trigger a rolling update, replacing the first job with the second.
    // Verify the first job is undeployed and the second job is running.
    cli("rolling-update", "--async", secondJobNameAndVersion, TEST_GROUP);
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);
    awaitTaskState(secondJobId, host, TaskStatus.State.RUNNING);
    final JobStatus status = client.jobStatus(firstJobId).get();
    assertThat(status.getDeployments().isEmpty(), is(true));

    // Stop the job
    cli("stop", secondJobNameAndVersion, host);
    awaitTaskState(secondJobId, host, TaskStatus.State.STOPPED);

    // Trigger a rolling update of the same job, and verify the job gets started. This takes
    // a different code path than when replacing a different job, which we tested above.
    cli("rolling-update", "--async", secondJobNameAndVersion, TEST_GROUP);
    awaitTaskState(secondJobId, host, TaskStatus.State.RUNNING);
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);
  }

  @Test
  public void testRollingUpdateWithOverlap() throws Exception {
    final List<String> hosts = ImmutableList.of(
        "dc1-" + testHost() + "-a1.dc1.example.com",
        "dc1-" + testHost() + "-a2.dc1.example.com",
        "dc2-" + testHost() + "-a1.dc2.example.com",
        "dc2-" + testHost() + "-a3.dc2.example.com",
        "dc3-" + testHost() + "-a4.dc3.example.com"
    );
    // start agents
    for (final String host : hosts) {
      startDefaultAgent(host, "--labels", TEST_LABEL);
    }

    // create a deployment group
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);

    // create and roll out a first job
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);
    cli("rolling-update", "--async", "--overlap", testJobNameAndVersion, TEST_GROUP);

    for (final String host : hosts) {
      awaitTaskState(jobId, host, TaskStatus.State.RUNNING);
    }
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);

    // create and roll out a second job
    final String secondJobVersion = testJobVersion + "2";
    final String secondJobNameAndVersion = testJobNameAndVersion + "2";
    final JobId secondJobId = createJob(testJobName, secondJobVersion, BUSYBOX, IDLE_COMMAND);
    cli("rolling-update", "--async", "--overlap", secondJobNameAndVersion, TEST_GROUP);

    for (final String host : hosts) {
      awaitTaskState(secondJobId, host, TaskStatus.State.RUNNING);
    }
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);
  }

  @Ignore
  @Test
  public void testRollingUpdatePerformance() throws Exception {
    final List<String> hosts = ImmutableList.of(
        "dc1-" + testHost() + "-a1.dc1.example.com",
        "dc1-" + testHost() + "-a2.dc1.example.com",
        "dc2-" + testHost() + "-a1.dc2.example.com",
        "dc2-" + testHost() + "-a3.dc2.example.com",
        "dc3-" + testHost() + "-a4.dc3.example.com"
    );

    // start agents
    for (final String host : hosts) {
      startDefaultAgent(host, "--labels", TEST_LABEL);
    }

    // Wait for agents to come up
    final HeliosClient client = defaultClient();
    for (final String host : hosts) {
      awaitHostStatus(client, host, UP, LONG_WAIT_SECONDS, SECONDS);
    }

    for (int i = 0; i < 50; ++i) {
      cli("create-deployment-group", "--json", TEST_GROUP + "-" + i, "tol=ahdsksajd");
      cli("rolling-update", "--async", testJobNameAndVersion, TEST_GROUP + "-" + i);
    }

    // create a deployment group and job
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);

    // TODO: fix this!
    // Wait for the host-updater
    Thread.sleep(2000);

    // trigger a rolling update
    cli("rolling-update", "--async", testJobNameAndVersion, TEST_GROUP);

    final long t0 = System.currentTimeMillis();

    // ensure the job is running on all agents and the deployment group reaches DONE
    for (final String host : hosts) {
      awaitTaskState(jobId, host, TaskStatus.State.RUNNING);
    }

    final Deployment deployment =
        defaultClient().hostStatus(hosts.get(0)).get().getJobs().get(jobId);
    assertEquals(TEST_GROUP, deployment.getDeploymentGroupName());
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP,
        DeploymentGroupStatus.State.DONE);

    System.out.printf("1 active / 0 inactive: Time to roll out: %.2f s\n",
        (System.currentTimeMillis() - t0) / 1000.0);
  }

  @Test
  public void testRollingUpdateWithOverlapAndParallelism() throws Exception {
    // create and start agents
    final List<String> hosts = ImmutableList.of(
        "dc1-" + testHost() + "-a1.dc1.example.com",
        "dc1-" + testHost() + "-a2.dc1.example.com",
        "dc2-" + testHost() + "-a1.dc2.example.com",
        "dc2-" + testHost() + "-a3.dc2.example.com",
        "dc3-" + testHost() + "-a4.dc3.example.com"
    );
    for (final String host : hosts) {
      startDefaultAgent(host, "--labels", TEST_LABEL);
    }

    // create a deployment group
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);

    // create and roll out a first job
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);
    cli("rolling-update", "--async", "-p", "2", "--overlap", testJobNameAndVersion, TEST_GROUP);

    for (final String host : hosts) {
      awaitTaskState(jobId, host, TaskStatus.State.RUNNING);
    }
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);

    // create and roll out a second job
    final String secondJobVersion = testJobVersion + "2";
    final String secondJobNameAndVersion = testJobNameAndVersion + "2";
    final JobId secondJobId = createJob(testJobName, secondJobVersion, BUSYBOX, IDLE_COMMAND);
    cli("rolling-update", "--async", "-p", "2", "--overlap", secondJobNameAndVersion, TEST_GROUP);

    for (final String host : hosts) {
      awaitTaskState(secondJobId, host, TaskStatus.State.RUNNING);
    }
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);
  }

  @Test
  public void testRollingUpdateWithToken() throws Exception {
    final String host = testHost();
    startDefaultAgent(host, "--labels", TEST_LABEL);

    // Wait for agent to come up
    final HeliosClient client = defaultClient();
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Manually deploy a job with a token on the host (i.e. a job not part of the deployment group)
    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setToken(TOKEN)
        .build();
    final JobId jobId = createJob(job);

    // Create a deployment-group and trigger a migration rolling-update
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);
    cli("rolling-update", "--async", "--token", TOKEN, testJobNameAndVersion, TEST_GROUP);

    // rolling-update should succeed & job should be running
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);
    awaitTaskState(jobId, host, TaskStatus.State.RUNNING);

    // Check that we cannot manually undeploy the job with a token
    final String output = cli("undeploy", jobId.toString(), host);
    assertThat(output, containsString("FORBIDDEN"));
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);
    awaitTaskState(jobId, host, TaskStatus.State.RUNNING);
  }

  @Test
  public void testRemoveDeploymentGroupInactive() throws Exception {
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);
    cli("rolling-update", "--async", "--migrate", testJobNameAndVersion, TEST_GROUP);

    final RemoveDeploymentGroupResponse response =
        defaultClient().removeDeploymentGroup(TEST_GROUP).get();

    assertEquals(RemoveDeploymentGroupResponse.Status.REMOVED, response.getStatus());
  }

  @Test
  public void testRemoveDeploymentGroupActive() throws Exception {
    final String host = testHost();
    startDefaultAgent(host, "--labels", TEST_LABEL);

    // Wait for agent to come up
    final HeliosClient client = defaultClient();
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Use an invalid image to make sure the DG doesn't succeed (this ensure the DG will be active
    // for 5+ minutes, which is plenty of time).
    final JobId jobId = createJob(testJobName, testJobVersion, "invalid_image", IDLE_COMMAND);
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);
    cli("rolling-update", "--async", "--migrate", testJobNameAndVersion, TEST_GROUP);

    final RemoveDeploymentGroupResponse response =
        defaultClient().removeDeploymentGroup(TEST_GROUP).get();

    assertEquals(RemoveDeploymentGroupResponse.Status.REMOVED, response.getStatus());
  }
}
