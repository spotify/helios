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

import com.google.common.base.Equivalence;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.agent.BoundedRandomExponentialBackoff;
import com.spotify.helios.agent.RetryScheduler;
import com.spotify.helios.servicescommon.DefaultReactor;
import com.spotify.helios.servicescommon.PersistentAtomicReference;
import com.spotify.helios.servicescommon.Reactor;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.common.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.MapDifference.ValueDifference;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.zookeeper.KeeperException.NoNodeException;
import static org.apache.zookeeper.KeeperException.NodeExistsException;

/**
 * A map that persists modification locally on disk and attempt to replicate modifications to
 * ZooKeeper, retrying forever until successful. Note that ZooKeeper is only written to and never
 * read from, so this is not a distributed map. Multiple changes to the same key are folded and
 * only the last value is written to ZooKeeper.
 */
public class ZooKeeperUpdatingPersistentDirectory extends AbstractIdleService {

  private static final Logger log =
      LoggerFactory.getLogger(ZooKeeperUpdatingPersistentDirectory.class);

  private static final long RETRY_INTERVAL_MILLIS = 5000;

  private static final Map<String, byte[]> EMPTY_ENTRIES = Collections.emptyMap();
  private static final TypeReference<Map<String, byte[]>> ENTRIES_TYPE =
      new TypeReference<Map<String, byte[]>>() {};

  private static final Equivalence<? super byte[]> BYTE_ARRAY_EQUIVALENCE =
      new Equivalence<byte[]>() {
        @Override
        protected boolean doEquivalent(final byte[] a, final byte[] b) {
          return Arrays.equals(a, b);
        }

        @Override
        protected int doHash(final byte[] bytes) {
          return Arrays.hashCode(bytes);
        }
      };

  private final ZooKeeperClientProvider provider;
  private final String path;
  private final Reactor reactor;
  private final PersistentAtomicReference<Map<String, byte[]>> entries;

  private final Object lock = new Object() {};

  private Map<String, byte[]> remote = Maps.newHashMap();
  private volatile boolean initialized;

  private final ConnectionStateListener connectionStateListener = new ConnectionStateListener() {
    @Override
    public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
      switch (newState) {
        case CONNECTED:
          break;
        case SUSPENDED:
          break;
        case RECONNECTED:
          initialized = false;
          reactor.signal();
          break;
        case LOST:
          break;
        case READ_ONLY:
          break;
      }
    }
  };

  private ZooKeeperUpdatingPersistentDirectory(final String name,
                                               final ZooKeeperClientProvider provider,
                                               final Path stateFile,
                                               final String path)
      throws IOException, InterruptedException {
    this.provider = provider;
    this.path = path;
    this.entries = PersistentAtomicReference.create(stateFile, ENTRIES_TYPE,
                                                    Suppliers.ofInstance(EMPTY_ENTRIES));
    this.reactor = new DefaultReactor(name, new Update(), RETRY_INTERVAL_MILLIS);
  }

  public byte[] put(final String key, final byte[] value) throws InterruptedException {
    Preconditions.checkArgument(key.indexOf('/') == -1);
    PathUtils.validatePath(ZKPaths.makePath(path, key));
    final byte[] prev;
    synchronized (lock) {
      final Map<String, byte[]> mutable = Maps.newHashMap(entries.get());
      prev = mutable.put(key, value);
      try {
        entries.set(ImmutableMap.copyOf(mutable));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    reactor.signal();
    return prev;
  }

  public byte[] remove(final Object key) throws InterruptedException {
    if (!(key instanceof String)) {
      return null;
    }
    return remove((String) key);
  }

  private byte[] remove(final String key) throws InterruptedException {
    Preconditions.checkArgument(key.indexOf('/') == -1);
    PathUtils.validatePath(ZKPaths.makePath(path, key));
    final byte[] value;
    synchronized (lock) {
      final Map<String, byte[]> mutable = Maps.newHashMap(entries.get());
      value = mutable.remove(key);
      try {
        entries.set(ImmutableMap.copyOf(mutable));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    reactor.signal();
    return value;
  }

  public byte[] get(final Object key) {
    return entries.get().get(key);
  }

  public Set<Map.Entry<String, byte[]>> entrySet() {
    return entries.get().entrySet();
  }

  private ZooKeeperClient client(final String tag) {
    return provider.get("persistent_directory_" + tag);
  }

  @Override
  protected void startUp() throws Exception {
    client("startUp").getConnectionStateListenable().addListener(connectionStateListener);
    reactor.startAsync().awaitRunning();
    reactor.signal();
  }

  @Override
  protected void shutDown() throws Exception {
    reactor.stopAsync().awaitTerminated();
  }

  public static ZooKeeperUpdatingPersistentDirectory create(final String name,
                                                            final ZooKeeperClientProvider client,
                                                            final Path stateFile,
                                                            final String path)
      throws IOException, InterruptedException {
    return new ZooKeeperUpdatingPersistentDirectory(name, client, stateFile, path);
  }


  private class Update implements Reactor.Callback {

    @Override
    public void run(final boolean timeout) throws InterruptedException {
      try {
        run0();
      } catch (KeeperException e) {
        throw Throwables.propagate(e);
      }
    }

    private void run0() throws KeeperException, InterruptedException {
      final RetryScheduler retryScheduler = BoundedRandomExponentialBackoff.newBuilder()
          .setMinInterval(1, SECONDS)
          .setMaxInterval(30, SECONDS)
          .build()
          .newScheduler();

      while (true) {
        try {
          if (!parentExists()) {
            log.warn("parent does not exist");
            return;
          }
          if (!initialized) {
            syncChecked();
            initialized = true;
          }
          incrementalUpdate();
          return;
        } catch (NodeExistsException | NoNodeException e) {
          final long backoff = retryScheduler.nextMillis();
          log.warn("conflict: {} {}. Resyncing in {}ms", e.getPath(), e.code(), backoff);
          initialized = false;
          Thread.sleep(backoff);
        }
      }
    }

    private void incrementalUpdate() throws KeeperException {
      final MapDifference<String, byte[]> difference = Maps.difference(entries.get(), remote,
                                                                       BYTE_ARRAY_EQUIVALENCE);
      if (difference.areEqual()) {
        return;
      }

      final Map<String, byte[]> newRemote = Maps.newHashMap(remote);

      final Map<String, byte[]> create = difference.entriesOnlyOnLeft();
      final Map<String, ValueDifference<byte[]>> update = difference.entriesDiffering();
      final Map<String, byte[]> delete = difference.entriesOnlyOnRight();

      log.debug("create: {}", create.keySet());
      log.debug("update: {}", update.keySet());
      log.debug("delete: {}", delete.keySet());

      for (final Map.Entry<String, byte[]> entry : create.entrySet()) {
        write(entry.getKey(), entry.getValue());
        newRemote.put(entry.getKey(), entry.getValue());
      }

      for (final Map.Entry<String, ValueDifference<byte[]>> entry : update.entrySet()) {
        write(entry.getKey(), entry.getValue().leftValue());
        newRemote.put(entry.getKey(), entry.getValue().leftValue());
      }

      for (final Map.Entry<String, byte[]> entry : delete.entrySet()) {
        delete(entry.getKey());
        newRemote.remove(entry.getKey());
      }

      remote = newRemote;
    }

    private boolean parentExists() {
      try {
        return client("parentExists").exists(path) != null;
      } catch (KeeperException e) {
        throw Throwables.propagate(e);
      }
    }

    private void delete(final String node) {
      final ZooKeeperClient client = client("delete");
      final String nodePath = ZKPaths.makePath(path, node);
      try {
        if (client.stat(nodePath) != null) {
          client.delete(nodePath);
          log.debug("Deleted node: {}", nodePath);
        }
      } catch (KeeperException.ConnectionLossException e) {
        log.warn("ZooKeeper connection lost while deleting node: {}", nodePath);
        throw Throwables.propagate(e);
      } catch (KeeperException e) {
        log.error("Failed to delete node: {}", nodePath, e);
        throw Throwables.propagate(e);
      }
    }

    private void write(final String node, final byte[] data) throws KeeperException {
      final ZooKeeperClient client = client("write");
      final String nodePath = ZKPaths.makePath(path, node);
      try {
        if (client.stat(nodePath) != null) {
          client.setData(nodePath, data);
          log.debug("Wrote node: {}", nodePath);
        } else {
          client.createAndSetData(nodePath, data);
          log.debug("Created node: {}", nodePath);
        }
      } catch (KeeperException.ConnectionLossException e) {
        log.warn("ZooKeeper connection lost while writing node: {}", nodePath);
        throw Throwables.propagate(e);
      }
    }

    private void syncChecked() throws KeeperException {
      final ZooKeeperClient client = client("sync");
      final List<String> nodes = client.getChildren(path);
      final Map<String, byte[]> snapshot = entries.get();

      // Get new remote state
      remote = Maps.newHashMap();
      for (String node : nodes) {
        final String nodePath = ZKPaths.makePath(path, node);
        final byte[] data = client.getData(nodePath);
        remote.put(node, data);
      }

      // Create and update missing and outdated nodes
      for (final Map.Entry<String, byte[]> entry : snapshot.entrySet()) {
        final String node = entry.getKey();
        final byte[] remoteData = remote.get(node);
        final byte[] localData = entry.getValue();
        final String nodePath = ZKPaths.makePath(path, node);
        if (remoteData == null) {
          log.debug("sync: creating node {}", nodePath);
          client.createAndSetData(nodePath, localData);
          remote.put(node, localData);
        } else if (!Arrays.equals(remoteData, localData)) {
          log.debug("sync: updating node {}", nodePath);
          client.setData(nodePath, localData);
          remote.put(node, localData);
        }
      }

      // Remove undesired nodes
      for (final String node : remote.keySet()) {
        if (!snapshot.containsKey(node)) {
          final String nodePath = ZKPaths.makePath(path, node);
          log.debug("sync: deleting node {}", nodePath);
          client.delete(nodePath);
          remote.remove(node);
        }
      }
    }
  }
}
