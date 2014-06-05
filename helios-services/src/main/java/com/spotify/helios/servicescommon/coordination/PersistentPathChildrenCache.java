/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.servicescommon.PersistentAtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode.POST_INITIALIZED_EVENT;

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
      cache.start(POST_INITIALIZED_EVENT);
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
    executorService.shutdownNow();
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
          mutated = addUpdateChild(newSnapshot, event.getData());
          break;
        }
        case CHILD_REMOVED: {
          final String path = event.getData().getPath();
          mutated = removeChild(path, newSnapshot);
          break;
        }
        case INITIALIZED: {
          final List<ChildData> initialData = event.getInitialData();
          final Set<String> initialPaths = Sets.newHashSet();
          for (final ChildData childData : initialData) {
            initialPaths.add(childData.getPath());
          }
          for (final ChildData data : initialData) {
            mutated |= addUpdateChild(newSnapshot, data);
          }
          final Set<String> currentPaths = ImmutableSet.copyOf(newSnapshot.keySet());
          for (final String path : currentPaths) {
            if (!initialPaths.contains(path)) {
              mutated |= removeChild(path, newSnapshot);
            }
          }
          break;
        }
        case CONNECTION_LOST:
          fireConnectionStateChanged(ConnectionState.LOST);
          break;
        case CONNECTION_RECONNECTED:
          fireConnectionStateChanged(ConnectionState.RECONNECTED);
          break;
        case CONNECTION_SUSPENDED:
          fireConnectionStateChanged(ConnectionState.SUSPENDED);
          break;
        default:
          throw new IllegalStateException();
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

    private boolean removeChild(final String path,
                                final Map<String, byte[]> newSnapshot) {
      if (newSnapshot.containsKey(path)) {
        newSnapshot.remove(path);
        return true;
      } else {
        return false;
      }
    }

    private boolean addUpdateChild(final Map<String, byte[]> newSnapshot,
                                   final ChildData data) {
      final String path = data.getPath();
      final byte[] currentData = newSnapshot.get(path);
      final byte[] newData = data.getData();
      if (!Arrays.equals(currentData, newData)) {
        newSnapshot.put(path, newData);
        return true;
      } else {
        return false;
      }
    }

  }

  private void fireConnectionStateChanged(final ConnectionState state) {
    for (final Listener listener : listeners) {
      try {
        listener.connectionStateChanged(state);
      } catch (Exception e) {
        log.error("Listener threw exception", e);
      }
    }
  }

  public interface Listener {

    void nodesChanged(PersistentPathChildrenCache cache);

    void connectionStateChanged(ConnectionState state);
  }
}
