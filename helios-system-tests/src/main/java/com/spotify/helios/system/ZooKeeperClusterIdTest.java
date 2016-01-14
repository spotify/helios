/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios.system;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.Container;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.hamcrest.CustomTypeSafeMatcher;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ZooKeeperClusterIdTest extends SystemTestBase {

  @Override
  public void baseSetup() throws Exception {
    super.baseSetup();
  }

  @Test
  public void testZooKeeperClient() throws Exception {
    // Create the cluster ID node
    zk().curatorWithSuperAuth().newNamespaceAwareEnsurePath(Paths.configId(zkClusterId))
          .ensure(zk().curatorWithSuperAuth().getZookeeperClient());

    // We need to create a new curator because ZooKeeperClient will try to start it,
    // and zk().curator() has already been started.
    final ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework curator = CuratorFrameworkFactory.builder()
        .retryPolicy(retryPolicy)
        .connectString(zk().connectString())
        .build();

    final ZooKeeperClient client = new DefaultZooKeeperClient(curator, zkClusterId);
    client.start();

    // This should work since the cluster ID exists
    client.create("/test");

    // Now let's remove the cluster ID
    client.delete(Paths.configId(zkClusterId));

    // Sleep so the watcher thread in ZooKeeperClient has a chance to update state
    Thread.sleep(500);

    // Try the same operation again, and it should fail this time
    try {
      client.ensurePath(Paths.configJobs());
      fail("ZooKeeper operation should have failed because cluster ID was removed");
    } catch (IllegalStateException ignore) {
    }
  }

  @Test
  public void testMaster() throws Exception {
    startDefaultMaster("--zk-cluster-id=" + zkClusterId);

    final HeliosClient client = defaultClient();

    // This should succeed since the cluster ID was created by SystemTestBase
    client.jobs().get();

    // Delete the cluster ID
    zk().curatorWithSuperAuth().delete().forPath(Paths.configId(zkClusterId));

    // Call jobs again, and this time it should throw an exception because the cluster ID is gone
    try {
      client.jobs().get();
    } catch (ExecutionException e) {
      assertThat(e.getMessage(), containsString("500"));
    }
  }

  @Test
  public void testAgent() throws Exception {
    startDefaultMaster("--zk-cluster-id=" + zkClusterId);
    startDefaultAgent(testHost(), "--zk-cluster-id=" + zkClusterId);
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Create job and deploy it
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);
    deployJob(jobId, testHost());
    final TaskStatus runningStatus = awaitTaskState(jobId, testHost(), RUNNING);
    final String containerId = runningStatus.getContainerId();

    // Delete the config node which contains the cluster ID and all the job definitions
    zk().curatorWithSuperAuth().delete().deletingChildrenIfNeeded().forPath("/config");

    // Sleep for a second so agent has a chance to react to deletion
    Thread.sleep(1000);

    // Make sure the agent didn't stop the job
    try (final DockerClient docker = getNewDockerClient()) {
      final List<Container> containers = docker.listContainers();
      final CustomTypeSafeMatcher<Container> containerIdMatcher =
          new CustomTypeSafeMatcher<Container>("Container with id " + containerId) {
        @Override
        protected boolean matchesSafely(Container container) {
          return container.id().equals(containerId);
        }
      };

      assertContainersMatch(containers, containerIdMatcher);
    }
  }

  @SuppressWarnings("unchecked")
  private void assertContainersMatch(final List<Container> containers,
      final CustomTypeSafeMatcher<Container> containerIdMatcher) {
    assertThat(containers, hasItems(new CustomTypeSafeMatcher[]{containerIdMatcher}));
  }

}
