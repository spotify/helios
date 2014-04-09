/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.servicescommon.coordination;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SettableFuture;

import com.spotify.helios.Polling;
import com.spotify.helios.ZooKeeperStandaloneServerManager;
import com.spotify.helios.Parallelized;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.state.ConnectionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.zookeeper.KeeperException.NoNodeException;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@RunWith(Parallelized.class)
public class PersistentPathChildrenCacheTest {

  private static final String PATH = "/foos";

  private ZooKeeperStandaloneServerManager zk = new ZooKeeperStandaloneServerManager();

  private PersistentPathChildrenCache cache;
  private Path directory;
  private Path stateFile;

  private PersistentPathChildrenCache.Listener listener =
      mock(PersistentPathChildrenCache.Listener.class);

  @Before
  public void setup() throws Exception {
    zk.ensure("/foos");
    directory = Files.createTempDirectory("helios-test");
    stateFile = directory.resolve("persistent-path-children-cache-test-nodes.json");
    startCache();
  }

  @After
  public void teardown() {
    FileUtils.deleteQuietly(directory.toFile());
    zk.close();
  }

  @Test
  public void verifyListenerCalledOnNodeAdd() throws Exception {
    zk.ensure("/foos/foo");
    verify(listener, timeout(60000).atLeastOnce()).nodesChanged(cache);
  }

  @Test
  public void verifyListenerCalledOnNodeRemoved() throws Exception {
    zk.ensure("/foos/foo");
    verify(listener, timeout(60000).atLeastOnce()).nodesChanged(cache);
    reset(listener);
    try {
      zk.curator().delete().forPath("/foos/foo");
    } catch (NoNodeException ignore) {
    }
    verify(listener, timeout(60000).atLeastOnce()).nodesChanged(cache);
  }

  @Test
  public void verifyNodesAreRetainedWhenZKGoesDown() throws Exception {
    cache = new PersistentPathChildrenCache(zk.curator(), PATH, stateFile);
    cache.addListener(listener);
    cache.startAsync().awaitRunning();

    // Create two nodes
    final String FOO1 = "/foos/foo1";
    final String FOO2 = "/foos/foo2";
    final Set<String> paths = ImmutableSet.of(FOO1, FOO2);
    for (String path : paths) {
      zk.ensure(path);
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

    assertEquals(paths, cache.getNodes().keySet());
  }

  @Test
  public void verifyNodesRemovedWhilePathChildrenCacheIsDownAreDetected() throws Exception {
    // Create two nodes
    final String FOO1 = "/foos/foo1";
    final String FOO2 = "/foos/foo2";
    final Set<String> paths = ImmutableSet.of(FOO1, FOO2);
    for (String path : paths) {
      zk.ensure(path);
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
      zk.curator().delete().forPath(FOO1);
    } catch (NoNodeException ignore) {
    }

    // Start the cache
    startCache();

    // Wait for the cache to reflect that there's only one node left
    final Set<String> postDeletePaths = ImmutableSet.of(FOO2);
    Polling.await(5, MINUTES, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return cache.getNodes().keySet().equals(postDeletePaths) ? true : null;
      }
    });

    verify(listener, atLeastOnce()).nodesChanged(cache);
  }

  private void startCache() throws IOException {
    reset(listener);
    cache = new PersistentPathChildrenCache(zk.curator(), PATH, stateFile);
    cache.addListener(listener);
    cache.startAsync().awaitRunning();
  }

  private void stopCache() {
    cache.stopAsync().awaitTerminated();
  }
}
