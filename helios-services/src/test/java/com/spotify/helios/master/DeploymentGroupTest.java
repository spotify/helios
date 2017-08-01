/*-
 * -\-\-
 * Helios Services
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

package com.spotify.helios.master;

import static com.spotify.helios.common.descriptors.DeploymentGroup.RollingUpdateReason.HOSTS_CHANGED;
import static com.spotify.helios.common.descriptors.DeploymentGroup.RollingUpdateReason.MANUAL;
import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.DONE;
import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.FAILED;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.set;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.DeploymentGroupTasks;
import com.spotify.helios.common.descriptors.HostSelector;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.common.descriptors.RolloutTask;
import com.spotify.helios.rollingupdate.RollingUndeployPlanner;
import com.spotify.helios.rollingupdate.RollingUpdatePlanner;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClientProvider;
import com.spotify.helios.servicescommon.coordination.ZooKeeperModelReporter;
import com.spotify.helios.servicescommon.coordination.ZooKeeperOperation;
import java.util.Collections;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.experimental.theories.suppliers.TestedOn;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

@RunWith(Theories.class)
public class DeploymentGroupTest {

  private static final String GROUP_NAME = "my_group";
  private TestingServer zkServer;
  private ZooKeeperClient client;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Captor
  private ArgumentCaptor<List<ZooKeeperOperation>> opCaptor;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    zkServer = new TestingServer(true);

    final CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
        .connectString(zkServer.getConnectString())
        .retryPolicy(new ExponentialBackoffRetry(100, 3))
        .build();
    client = new DefaultZooKeeperClient(curatorFramework);
    client.start();
  }

  @After
  public void tearDown() throws Exception {
    client.close();
    zkServer.close();
  }

  private ZooKeeperMasterModel newMasterModel(final ZooKeeperClient client) {
    return new ZooKeeperMasterModel(
        new ZooKeeperClientProvider(client, ZooKeeperModelReporter.noop()),
        getClass().getName(),
        Collections.emptyList(),
        "");
  }

  // A test that ensures healthy deployment groups will perform a rolling update when their hosts
  // change.
  @Test
  public void testUpdateDeploymentGroupHosts() throws Exception {
    final ZooKeeperClient client = spy(this.client);
    final ZooKeeperMasterModel masterModel = spy(newMasterModel(client));

    // Return a job so we can add a real deployment group.
    final Job job = Job.newBuilder()
        .setCommand(ImmutableList.of("COMMAND"))
        .setImage("IMAGE")
        .setName("JOB_NAME")
        .setVersion("VERSION")
        .build();
    doReturn(job).when(masterModel).getJob(job.getId());

    // Add a real deployment group.
    final DeploymentGroup dg = DeploymentGroup.newBuilder()
        .setName(GROUP_NAME)
        .setHostSelectors(ImmutableList.of(HostSelector.parse("role=melmac")))
        .setJobId(job.getId())
        .setRolloutOptions(RolloutOptions.newBuilder().build())
        .setRollingUpdateReason(MANUAL)
        .build();
    masterModel.addDeploymentGroup(dg);

    // Setup some hosts
    final String oldHost = "host1";
    final String newHost = "host2";
    client.ensurePath(Paths.configHost(oldHost));
    client.ensurePath(Paths.configHost(newHost));
    client.ensurePath(Paths.statusHostUp(oldHost));
    client.ensurePath(Paths.statusHostUp(newHost));

    // Give the deployment group a host.
    client.setData(
        Paths.statusDeploymentGroupHosts(dg.getName()),
        Json.asBytes(ImmutableList.of(oldHost)));

    // And a status...
    client.setData(
        Paths.statusDeploymentGroup(dg.getName()),
        DeploymentGroupStatus.newBuilder().setState(DONE).build().toJsonBytes()
    );

    // Switch out our host!
    // TODO(negz): Use an unchanged host, make sure ordering remains the same.
    masterModel.updateDeploymentGroupHosts(dg.getName(), ImmutableList.of(newHost));

    verify(client, times(2)).transaction(opCaptor.capture());

    final DeploymentGroup changed = dg.toBuilder()
        .setRollingUpdateReason(HOSTS_CHANGED)
        .build();

    // Ensure we set the DG status to HOSTS_CHANGED.
    // This means we triggered a rolling update.
    final ZooKeeperOperation setDeploymentGroupHostChanged = set(
        Paths.configDeploymentGroup(dg.getName()),
        changed);

    // Ensure ZK tasks are written to:
    // - Perform a rolling undeploy for the removed (old) host
    // - Perform a rolling update for the added (new) host and the unchanged host
    final List<RolloutTask> tasks = ImmutableList.<RolloutTask>builder()
        .addAll(RollingUndeployPlanner.of(changed).plan(singletonList(oldHost)))
        .addAll(RollingUpdatePlanner.of(changed).plan(singletonList(newHost)))
        .build();

    final ZooKeeperOperation setDeploymentGroupTasks = set(
        Paths.statusDeploymentGroupTasks(dg.getName()),
        DeploymentGroupTasks.newBuilder()
            .setRolloutTasks(tasks)
            .setTaskIndex(0)
            .setDeploymentGroup(changed)
            .build()
    );
    assertThat(opCaptor.getValue(),
        hasItems(setDeploymentGroupHostChanged, setDeploymentGroupTasks));
  }

  // A test that ensures deployment groups that failed during a rolling update triggered by
  // changing hosts will perform a new rolling update if the hosts change again.
  @Test
  public void testUpdateFailedHostsChangedDeploymentGroupHosts() throws Exception {
    final ZooKeeperClient client = spy(this.client);
    final ZooKeeperMasterModel masterModel = spy(newMasterModel(client));

    // Return a job so we can add a real deployment group.
    final Job job = Job.newBuilder()
        .setCommand(ImmutableList.of("COMMAND"))
        .setImage("IMAGE")
        .setName("JOB_NAME")
        .setVersion("VERSION")
        .build();
    doReturn(job).when(masterModel).getJob(job.getId());

    // Add a real deployment group.
    final DeploymentGroup dg = DeploymentGroup.newBuilder()
        .setName(GROUP_NAME)
        .setHostSelectors(ImmutableList.of(HostSelector.parse("role=melmac")))
        .setJobId(job.getId())
        .setRolloutOptions(RolloutOptions.newBuilder().build())
        .setRollingUpdateReason(HOSTS_CHANGED)
        .build();
    masterModel.addDeploymentGroup(dg);

    // Give the deployment group a host.
    client.setData(
        Paths.statusDeploymentGroupHosts(dg.getName()),
        Json.asBytes(ImmutableList.of("host1")));

    // And a status...
    client.setData(
        Paths.statusDeploymentGroup(dg.getName()),
        DeploymentGroupStatus.newBuilder().setState(FAILED).build().toJsonBytes()
    );

    // Pretend our new host is UP.
    final HostStatus statusUp = mock(HostStatus.class);
    doReturn(HostStatus.Status.UP).when(statusUp).getStatus();
    doReturn(statusUp).when(masterModel).getHostStatus("host2");

    // Switch out our host!
    masterModel.updateDeploymentGroupHosts(dg.getName(), ImmutableList.of("host2"));

    // Ensure we write the same DG status again.
    // This is a no-op, but it means we triggered a rolling update.
    final ZooKeeperOperation setDeploymentGroup = set(
        Paths.configDeploymentGroup(dg.getName()), dg);
    verify(client, times(2)).transaction(opCaptor.capture());
    assertThat(opCaptor.getValue(), hasItem(setDeploymentGroup));
  }

  // A test that ensures deployment groups that failed during a manual rolling update will not
  // perform a new rolling update if the hosts change.
  @Test
  public void testUpdateFailedManualDeploymentGroupHosts() throws Exception {
    final ZooKeeperClient client = spy(this.client);
    final ZooKeeperMasterModel masterModel = spy(newMasterModel(client));

    // Return a job so we can add a real deployment group.
    final Job job = Job.newBuilder()
        .setCommand(ImmutableList.of("COMMAND"))
        .setImage("IMAGE")
        .setName("JOB_NAME")
        .setVersion("VERSION")
        .build();
    doReturn(job).when(masterModel).getJob(job.getId());

    // Add a real deployment group.
    final DeploymentGroup dg = DeploymentGroup.newBuilder()
        .setName(GROUP_NAME)
        .setHostSelectors(ImmutableList.of(HostSelector.parse("role=melmac")))
        .setJobId(job.getId())
        .setRolloutOptions(RolloutOptions.newBuilder().build())
        .setRollingUpdateReason(MANUAL)
        .build();
    masterModel.addDeploymentGroup(dg);

    // Give the deployment group a host.
    client.setData(
        Paths.statusDeploymentGroupHosts(dg.getName()),
        Json.asBytes(ImmutableList.of("host1")));

    // And a status...
    client.setData(
        Paths.statusDeploymentGroup(dg.getName()),
        DeploymentGroupStatus.newBuilder().setState(FAILED).build().toJsonBytes()
    );

    // Pretend our new host is UP.
    final HostStatus statusUp = mock(HostStatus.class);
    doReturn(HostStatus.Status.UP).when(statusUp).getStatus();
    doReturn(statusUp).when(masterModel).getHostStatus("host2");

    // Switch out our host!
    masterModel.updateDeploymentGroupHosts(dg.getName(), ImmutableList.of("host2"));

    // Ensure we do not set the DG status to HOSTS_CHANGED.
    // We don't want to trigger a rolling update because the last one was manual, and failed.
    final ZooKeeperOperation setDeploymentGroupHostChanged = set(
        Paths.configDeploymentGroup(dg.getName()),
        dg.toBuilder().setRollingUpdateReason(HOSTS_CHANGED).build()
    );
    verify(client, times(2)).transaction(opCaptor.capture());
    assertThat(opCaptor.getValue(), not(hasItem(setDeploymentGroupHostChanged)));
  }

  // A test that...
  // * Verifies that the state in ZK is correct after running stop
  // * Verifies that the correct exception is thrown when the DG does not exist or there is a
  //   race condition
  @Theory
  public void testStopDeploymentGroup(
      @TestedOn(ints = { 0, 1 }) final int dgExistsInt,
      @TestedOn(ints = { 0, 1 }) final int tasksExistInt,
      @TestedOn(ints = { 0, 1 }) final int tasksExistWhenCommittingInt
  ) throws Exception {
    final boolean dgExists = dgExistsInt != 0;
    final boolean tasksExist = tasksExistInt != 0;
    final boolean tasksExistWhenCommitting = tasksExistWhenCommittingInt != 0;

    // To be able to simulate triggering the race condition in stopDeploymentGroup we need to do
    // some mocking, relying on that the implementation uses client.exists() to check for the
    // presence of tasks.
    final ZooKeeperClient client = spy(this.client);
    when(client.exists(Paths.statusDeploymentGroupTasks(GROUP_NAME)))
        .thenReturn(tasksExist ? mock(Stat.class) : null);

    final ZooKeeperMasterModel masterModel = newMasterModel(client);

    if (dgExists) {
      final DeploymentGroup dg = DeploymentGroup.newBuilder()
          .setName(GROUP_NAME)
          .build();
      masterModel.addDeploymentGroup(dg);
    }

    if (tasksExistWhenCommitting) {
      client.ensurePath(Paths.statusDeploymentGroupTasks());
      client.create(Paths.statusDeploymentGroupTasks(GROUP_NAME));
    }

    if (!dgExists) {
      exception.expect(DeploymentGroupDoesNotExistException.class);
    } else if (tasksExist != tasksExistWhenCommitting) {
      exception.expect(HeliosRuntimeException.class);
    }

    masterModel.stopDeploymentGroup(GROUP_NAME);

    // Verify that the state in ZK is correct:
    // * tasks are not present
    // * the status is set to FAILED
    //
    // When checking for the existence of the tasks make sure we use the client that doesn't have
    // the exists() method mocked out!
    assertNull(this.client.exists(Paths.statusDeploymentGroupTasks(GROUP_NAME)));
    final DeploymentGroupStatus status = masterModel.getDeploymentGroupStatus(GROUP_NAME);
    assertEquals(FAILED, status.getState());
  }

}
