/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.common.coordination;

import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.common.PersistentAtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.SECONDS;

public class PersistentPathChildrenCache extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(PersistentPathChildrenCache.class);

  private static final long RETRY_INTERVAL = 5000;

  private static final Map<String, byte[]> EMPTY_NODES = Collections.emptyMap();
  private static final TypeReference<Map<String, byte[]>> NODES_TYPE =
      new TypeReference<Map<String, byte[]>>() {};

  private final PathChildrenCache cache;
  private final PersistentAtomicReference<Map<String, byte[]>> snapshot;

  private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

  private final ExecutorService executorService = MoreExecutors.getExitingExecutorService(
      (ThreadPoolExecutor) Executors.newFixedThreadPool(1), 0, SECONDS);

  public PersistentPathChildrenCache(final CuratorFramework curator, final String path,
                                     final Path snapshotFile)
      throws IOException {
    this.cache = new PathChildrenCache(curator, path, true, false, executorService);
    this.snapshot = PersistentAtomicReference.create(snapshotFile, NODES_TYPE,
                                                     Suppliers.ofInstance(EMPTY_NODES));
    cache.getListenable().addListener(new CacheListener());
  }

  public void addListener(final Listener listener) {
    listeners.add(listener);
  }

  public void removeListener(final Listener listener) {
    listeners.remove(listener);
  }

  @Override
  protected void startUp() throws Exception {
    log.debug("starting cache");
    try {
      cache.start();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      cache.close();
    } catch (IOException e) {
      log.error("Failed to close cache", e);
    }
    executorService.awaitTermination(5, SECONDS);
  }

  public Map<String, byte[]> getNodes() {
    return snapshot.get();
  }

  private class CacheListener implements PathChildrenCacheListener {

    @Override
    public void childEvent(final CuratorFramework client, final PathChildrenCacheEvent event)
        throws Exception {

      log.debug("cache event: {} {}", event.getType(), event.getData() == null
                                                       ? ""
                                                       : event.getData().getPath());

      final Map<String, byte[]> newSnapshot = Maps.newHashMap(snapshot.get());
      boolean mutated = false;

      switch (event.getType()) {
        case CHILD_ADDED:
        case CHILD_UPDATED: {
          final String path = event.getData().getPath();
          final byte[] currentData = newSnapshot.get(path);
          final byte[] newData = event.getData().getData();
          if (!Arrays.equals(currentData, newData)) {
            newSnapshot.put(path, newData);
            mutated = true;
          }
          break;
        }
        case CHILD_REMOVED: {
          newSnapshot.remove(event.getData().getPath());
          mutated = true;
          break;
        }
        case INITIALIZED:
        case CONNECTION_LOST:
        case CONNECTION_RECONNECTED:
        case CONNECTION_SUSPENDED:
          // ignored
          break;
      }

      if (mutated) {
        while (true) {
          try {
            snapshot.set(ImmutableMap.copyOf(newSnapshot));
            break;
          } catch (IOException e) {
            log.error("Failed to write cache snapshot: {}", snapshot);
            Thread.sleep(RETRY_INTERVAL);
          }
        }

        for (final Listener listener : listeners) {
          try {
            listener.nodesChanged(PersistentPathChildrenCache.this);
          } catch (Exception e) {
            log.error("Listener threw exception", e);
          }
        }
      }
    }

  }

  public interface Listener {

    void nodesChanged(PersistentPathChildrenCache cache);
  }
}
