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

package com.spotify.helios.servicescommon.coordination;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.SettableFuture;

import com.spotify.helios.Parallelized;
import com.spotify.helios.Polling;
import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.ZooKeeperTestingServerManager;
import com.spotify.helios.common.Json;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.zookeeper.KeeperException.NoNodeException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@RunWith(Parallelized.class)
public class PersistentPathChildrenCacheTest {

  public static class DataPojo {

    public DataPojo() {
    }

    public DataPojo(final String name) {
      this.name = name;
    }

    public String name;
    public int bar = 17;
    public Map<String, String> baz = ImmutableMap.of("foos", "bars");

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final DataPojo dataPojo = (DataPojo) o;

      if (bar != dataPojo.bar) {
        return false;
      }
      if (baz != null ? !baz.equals(dataPojo.baz) : dataPojo.baz != null) {
        return false;
      }
      if (name != null ? !name.equals(dataPojo.name) : dataPojo.name != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + bar;
      result = 31 * result + (baz != null ? baz.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("name", name)
          .add("bar", bar)
          .add("baz", baz)
          .toString();
    }
  }

  private static final String PATH = "/foos";

  private ZooKeeperTestManager zk;

  private PersistentPathChildrenCache<DataPojo> cache;
  private Path directory;
  private Path stateFile;

  private PersistentPathChildrenCache.Listener listener =
      mock(PersistentPathChildrenCache.Listener.class);

  @Before
  public void setup() throws Exception {
    zk = new ZooKeeperTestingServerManager();
    zk.ensure("/foos");
    directory = Files.createTempDirectory("helios-test");
    stateFile = directory.resolve("persistent-path-children-cache-test-nodes.json");
    startCache();
  }

  @After
  public void teardown() throws Exception {
    FileUtils.deleteQuietly(directory.toFile());
    zk.close();
  }

  @Test
  public void verifyListenerCalledOnNodeAdd() throws Exception {
    final DataPojo created = new DataPojo("foo");
    ensure("/foos/foo", created);
    verify(listener, timeout(60000).atLeastOnce()).nodesChanged(cache);
    final DataPojo read = Iterables.getOnlyElement(cache.getNodes().values());
    assertEquals(created, read);
  }

  @Test
  public void verifyListenerCalledOnNodeChange() throws Exception {
    final DataPojo created = new DataPojo("foo");
    ensure("/foos/foo", created);
    verify(listener, timeout(60000).atLeastOnce()).nodesChanged(cache);
    reset(listener);
    final DataPojo changed = new DataPojo("foo-changed");
    zk.curatorWithSuperAuth().setData().forPath("/foos/foo", Json.asBytesUnchecked(changed));
    verify(listener, timeout(60000).atLeastOnce()).nodesChanged(cache);
    final DataPojo read = Iterables.getOnlyElement(cache.getNodes().values());
    assertEquals(changed, read);
  }

  @Test
  public void verifyListenerCalledOnNodeRemoved() throws Exception {
    ensure("/foos/foo", new DataPojo("foo"));
    verify(listener, timeout(60000).atLeastOnce()).nodesChanged(cache);
    reset(listener);
    try {
      zk.curatorWithSuperAuth().delete().forPath("/foos/foo");
    } catch (NoNodeException ignore) {
    }
    verify(listener, timeout(60000).atLeastOnce()).nodesChanged(cache);
    assertTrue(cache.getNodes().isEmpty());
  }

  @Test
  public void verifyNodesAreRetainedWhenZKGoesDown() throws Exception {
    // Create two nodes
    final String foo1 = "/foos/foo1";
    final String foo2 = "/foos/foo2";
    final Set<String> paths = ImmutableSet.of(foo1, foo2);
    for (String path : paths) {
      ensure(path, new DataPojo(path));
    }

    // Wait for the cache to pick them up
    Polling.await(5, MINUTES, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return cache.getNodes().keySet().equals(paths) ? true : null;
      }
    });

    verify(listener, atLeastOnce()).nodesChanged(cache);

    // Take down zk
    zk.stop();

    // Wait for connection to be lost
    final SettableFuture<Void> connectionLost = SettableFuture.create();
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
        if (invocationOnMock.getArguments()[0] == ConnectionState.LOST) {
          connectionLost.set(null);
        }
        return null;
      }
    }).when(listener).connectionStateChanged(any(ConnectionState.class));
    connectionLost.get(5, MINUTES);

    // Keep probing for 30 seconds to build some confidence that the snapshot is not going away
    for (int i = 0; i < 30; i++) {
      Thread.sleep(1000);
      assertEquals(paths, cache.getNodes().keySet());
    }
  }

  @Test
  public void verifyNodesRemovedWhilePathChildrenCacheIsDownAreDetected() throws Exception {
    // Create two nodes
    final String foo1 = "/foos/foo1";
    final String foo2 = "/foos/foo2";
    final Set<String> paths = ImmutableSet.of(foo1, foo2);
    for (String path : paths) {
      ensure(path, new DataPojo(path));
    }

    // Wait for the cache to pick them up
    Polling.await(5, MINUTES, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return cache.getNodes().keySet().equals(paths) ? true : null;
      }
    });

    verify(listener, atLeastOnce()).nodesChanged(cache);

    // Stop the cache
    stopCache();

    // Remove a node
    try {
      zk.curatorWithSuperAuth().delete().forPath(foo1);
    } catch (NoNodeException ignore) {
    }

    // Start the cache
    startCache();

    // Wait for the cache to reflect that there's only one node left
    final Set<String> postDeletePaths = ImmutableSet.of(foo2);
    Polling.await(5, MINUTES, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return cache.getNodes().keySet().equals(postDeletePaths) ? true : null;
      }
    });

    verify(listener, atLeastOnce()).nodesChanged(cache);
  }

  private void startCache() throws IOException, InterruptedException {
    reset(listener);
    cache = new PersistentPathChildrenCache<>(
        zk.curatorWithSuperAuth(), PATH, null, stateFile, Json.type(DataPojo.class));
    cache.addListener(listener);
    cache.startAsync().awaitRunning();
  }

  private void stopCache() {
    cache.stopAsync().awaitTerminated();
  }

  private void ensure(final String path, final Object value) throws Exception {
    zk.ensure(ZKPaths.getPathAndNode(path).getPath());
    try {
      zk.curatorWithSuperAuth().create().forPath(path, Json.asBytesUnchecked(value));
    } catch (KeeperException.NodeExistsException ignore) {
    }

  }

}
