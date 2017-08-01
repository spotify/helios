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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.servicescommon.coordination.CuratorClientFactory;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryLoop;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MasterRespondsWithNoZkTest extends SystemTestBase {

  @Override
  protected ZooKeeperTestManager zooKeeperTestManager() {
    final ZooKeeperTestManager testManager = mock(ZooKeeperTestManager.class);
    final MockCuratorClientFactory mockCuratorClientFactory = new MockCuratorClientFactory();
    final CuratorFramework curator = mockCuratorClientFactory.newClient(
        null, 0, 0, null, null, null);
    when(testManager.curatorWithSuperAuth()).thenReturn(curator);

    when(testManager.connectString()).thenReturn("127.0.0.1");
    return testManager;
  }

  @Test
  public void test() throws Exception {

    startDefaultMasterDontWaitForZk(new MockCuratorClientFactory(), "--zk-connection-timeout", "1");

    final HeliosClient client = defaultClient();

    try {
      final String result = client.listMasters().get().get(0);

      fail("Exception should have been thrown, as ZK doesnt exist - got " + result);

    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof HeliosException);
    }

  }

  private static class MockCuratorClientFactory implements CuratorClientFactory {

    @Override
    public CuratorFramework newClient(final String connectString, final int sessionTimeoutMs,
                                      final int connectionTimeoutMs, final RetryPolicy retryPolicy,
                                      final ACLProvider aclProvider,
                                      final List<AuthInfo> authorization) {
      final CuratorFramework curator = mock(CuratorFramework.class);

      final RetryLoop retryLoop = mock(RetryLoop.class);
      when(retryLoop.shouldContinue()).thenReturn(false);

      final CuratorZookeeperClient czkClient = mock(CuratorZookeeperClient.class);
      when(czkClient.newRetryLoop()).thenReturn(retryLoop);

      when(curator.getZookeeperClient()).thenReturn(czkClient);

      @SuppressWarnings("unchecked") final Listenable<ConnectionStateListener> mockListener =
          (Listenable<ConnectionStateListener>) mock(Listenable.class);

      when(curator.getConnectionStateListenable()).thenReturn(mockListener);

      final GetChildrenBuilder builder = mock(GetChildrenBuilder.class);
      when(curator.getChildren()).thenReturn(builder);

      try {
        when(builder.forPath(anyString())).thenThrow(
            new KeeperException.ConnectionLossException());
      } catch (Exception ignored) {
        // never throws
      }
      when(curator.newNamespaceAwareEnsurePath(anyString())).thenReturn(mock(EnsurePath.class));

      return curator;
    }
  }
}
