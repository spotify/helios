/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.servicescommon.coordination;

import com.google.common.collect.ImmutableSet;

import com.spotify.helios.Polling;
import com.spotify.helios.ZooKeeperStandaloneServerManager;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.KeeperException.NoNodeException;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class PersistentPathChildrenCacheTest {

  private static final String PATH = "/foos";

  private ZooKeeperStandaloneServerManager zk = new ZooKeeperStandaloneServerManager();

  private PersistentPathChildrenCache cache;
  private Path directory;
  private Path stateFile;

  @Mock public PersistentPathChildrenCache.Listener listener;

  @Before
  public void setup() throws Exception {
    zk.ensure("/foos");
    directory = Files.createTempDirectory("helios-test");
    stateFile = directory.resolve("persistent-path-children-cache-test-nodes.json");
  }

  @After
  public void teardown() {
    FileUtils.deleteQuietly(directory.toFile());
    zk.close();
  }

  @Test
  public void verifyNodesRemovedWhilePathChildrenCacheIsDownAreDetected() throws Exception {
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
    Polling.await(5, TimeUnit.MINUTES, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return cache.getNodes().keySet().equals(paths) ? true : null;
      }
    });

    verify(listener, atLeastOnce()).nodesChanged(cache);

    // Stop the cache
    cache.stopAsync().awaitTerminated();
    reset(listener);

    // Remove a node
    try {
      zk.curator().delete().forPath(FOO1);
    } catch (NoNodeException ignore) {
    }

    // Start the cache
    cache = new PersistentPathChildrenCache(zk.curator(), PATH, stateFile);
    cache.addListener(listener);
    cache.startAsync().awaitRunning();

    // Wait for the cache to reflect that there's only one node left
    final Set<String> postDeletePaths = ImmutableSet.of(FOO2);
    Polling.await(5, TimeUnit.MINUTES, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return cache.getNodes().keySet().equals(postDeletePaths) ? true : null;
      }
    });

    verify(listener, atLeastOnce()).nodesChanged(cache);
  }

}
