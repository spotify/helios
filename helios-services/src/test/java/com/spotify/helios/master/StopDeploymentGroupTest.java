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

import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
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
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.experimental.theories.suppliers.TestedOn;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(Theories.class)
public class StopDeploymentGroupTest {

  private static final String GROUP_NAME = "my_group";
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

  // A test that...
  // * Verifies that the state in ZK is correct after running stop
  // * Verifies that the correct exception is thrown when the DG does not exist or there is a
  //   race condition
  @Theory
  public void testStopDeploymentGroup(
      @TestedOn(ints = {0, 1}) final int dgExistsInt,
      @TestedOn(ints = {0, 1}) final int tasksExistInt,
      @TestedOn(ints = {0, 1}) final int tasksExistWhenCommittingInt
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

    final ZooKeeperMasterModel masterModel = new ZooKeeperMasterModel(
        new ZooKeeperClientProvider(client, ZooKeeperModelReporter.noop()),
        getClass().getName(),
        mock(KafkaSender.class));

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
    assertEquals(DeploymentGroupStatus.State.FAILED, status.getState());
  }

}
