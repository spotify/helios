/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.master;

import com.google.common.collect.ImmutableList;

import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.HostSelector;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.RollingOperation;
import com.spotify.helios.common.descriptors.RollingOperationStatus;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.servicescommon.KafkaSender;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClientProvider;
import com.spotify.helios.servicescommon.coordination.ZooKeeperModelReporter;

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

import java.util.List;

import static com.spotify.helios.common.descriptors.RollingOperationStatus.State.NEW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(Theories.class)
public class RollingOperationTest {

  private static final String ROLLING_OP_ID = "uuid";
  private TestingServer zkServer;
  private ZooKeeperClient client;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
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

  // A test that verifies we only keep MAX_ROLLING_OPERATION_HISTORY rolling operations per
  // deployment group in ZK.
  @Test
  public void testRollingOperationHistoryTruncated() throws Exception {
    final ZooKeeperClient client = spy(this.client);
    final ZooKeeperMasterModel masterModel = spy(new ZooKeeperMasterModel(
        new ZooKeeperClientProvider(client, ZooKeeperModelReporter.noop()),
        getClass().getName(),
        mock(KafkaSender.class)));

    final Job job = Job.newBuilder()
        .setCommand(ImmutableList.of("COMMAND"))
        .setImage("IMAGE")
        .setName("JOB_NAME")
        .setVersion("VERSION")
        .build();

    final DeploymentGroup dg = new DeploymentGroup(
        "my_group", ImmutableList.of(HostSelector.parse("role=foo")));
    final RolloutOptions options = RolloutOptions.newBuilder().build();

    doReturn(job).when(masterModel).getJob(job.getId());
    doReturn(ImmutableList.of()).when(masterModel).getDeploymentGroupHosts(dg.getName());

    masterModel.addDeploymentGroup(dg);

    for (int i = 0; i < 11; i++) {
      masterModel.rollingUpdate(dg, job.getId(), options);
    }

    final List<RollingOperation> ops = masterModel.getRollingOperations(dg.getName());
    assertEquals(ops.size(), ZooKeeperMasterModel.MAX_ROLLING_OPERATION_HISTORY);
    assertEquals(ops.get(0), masterModel.getLastRollingOperation(dg.getName()));

    for (final RollingOperation rolling: ops) {
      final RollingOperationStatus status = masterModel.getRollingOperationStatus(rolling.getId());
      assertEquals(rolling.getJobId(), job.getId());
      assertEquals(status.getState(), RollingOperationStatus.State.DONE);
    }
  }

  // A test that...
  // * Verifies that the state in ZK is correct after running stop
  // * Verifies that the correct exception is thrown when the op does not exist or there is a
  //   race condition
  @Theory
  public void testStopRollingOperation(
      @TestedOn(ints = {0, 1}) final int opExistsInt,
      @TestedOn(ints = {0, 1}) final int tasksExistInt,
      @TestedOn(ints = {0, 1}) final int tasksExistWhenCommittingInt
  ) throws Exception {
    final boolean opExists = opExistsInt != 0;
    final boolean tasksExist = tasksExistInt != 0;
    final boolean tasksExistWhenCommitting = tasksExistWhenCommittingInt != 0;

    // To be able to simulate triggering the race condition in stopRollingOperation we need to do
    // some mocking, relying on that the implementation uses client.exists() to check for the
    // presence of tasks.
    final ZooKeeperClient client = spy(this.client);
    when(client.exists(Paths.statusRollingOpsTasks(ROLLING_OP_ID)))
        .thenReturn(tasksExist ? mock(Stat.class) : null);

    final ZooKeeperMasterModel masterModel = new ZooKeeperMasterModel(
        new ZooKeeperClientProvider(client, ZooKeeperModelReporter.noop()),
        getClass().getName(),
        mock(KafkaSender.class));

    if (opExists) {
      final RollingOperation rolling = RollingOperation.newBuilder()
          .setId(ROLLING_OP_ID)
          .build();

      final RollingOperationStatus status = RollingOperationStatus.newBuilder()
          .setState(NEW)
          .build();

      client.ensurePath(Paths.configRollingOps());
      client.ensurePath(Paths.statusRollingOps());
      client.createAndSetData(Paths.configRollingOp(ROLLING_OP_ID), rolling.toJsonBytes());
      client.createAndSetData(Paths.statusRollingOp(ROLLING_OP_ID), status.toJsonBytes());
    }

    if (tasksExistWhenCommitting) {
      client.ensurePath(Paths.statusRollingOpsTasks());
      client.create(Paths.statusRollingOpsTasks(ROLLING_OP_ID));
    }

    if (!opExists) {
      exception.expect(RollingOperationDoesNotExistException.class);
    } else if (tasksExist != tasksExistWhenCommitting) {
      exception.expect(HeliosRuntimeException.class);
    }

    masterModel.stopRollingOperation(ROLLING_OP_ID);

    // Verify that the state in ZK is correct:
    // * tasks are not present
    // * the status is set to FAILED
    //
    // When checking for the existence of the tasks make sure we use the client that doesn't have
    // the exists() method mocked out!
    assertNull(this.client.exists(Paths.statusRollingOpsTasks(ROLLING_OP_ID)));
    final RollingOperationStatus status = masterModel.getRollingOperationStatus(ROLLING_OP_ID);
    assertEquals(RollingOperationStatus.State.FAILED, status.getState());
  }

}
