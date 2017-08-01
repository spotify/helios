/*-
 * -\-\-
 * Helios Services
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

package com.spotify.helios.servicescommon.coordination;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.util.concurrent.Service.State.STOPPING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.MapType;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.spotify.helios.agent.BoundedRandomExponentialBackoff;
import com.spotify.helios.agent.RetryIntervalPolicy;
import com.spotify.helios.agent.RetryScheduler;
import com.spotify.helios.common.Json;
import com.spotify.helios.servicescommon.DefaultReactor;
import com.spotify.helios.servicescommon.PersistentAtomicReference;
import com.spotify.helios.servicescommon.Reactor;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A view of the children of a zookeeper node, kept up to date with zookeeper using watches and
 * persisted to disk in order to guarantee availability when zookeeper is unavailable.
 *
 * <p>The view is persisted to disk as json and the node values must be valid json.
 *
 * @param <T> The deserialized node value type.
 */
public class PersistentPathChildrenCache<T> extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(PersistentPathChildrenCache.class);

  private static final long REFRESH_INTERVAL_MILLIS = 30000;

  private final PersistentAtomicReference<Map<String, T>> snapshot;
  private final CuratorFramework curator;
  private final String path;
  private final String clusterId;
  private final JavaType valueType;

  private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();
  private final CuratorWatcher childrenWatcher = new ChildrenWatcher();
  private final CuratorWatcher dataWatcher = new DataWatcher();
  private final Set<String> changes = Sets.newConcurrentHashSet();
  private final Reactor reactor;

  private volatile boolean synced;

  public PersistentPathChildrenCache(final CuratorFramework curator, final String path,
                                     final String clusterId, final Path snapshotFile,
                                     final JavaType valueType)
      throws IOException, InterruptedException {
    this.curator = curator;
    this.path = path;
    this.clusterId = clusterId;
    this.valueType = valueType;

    final MapType mapType = Json.typeFactory().constructMapType(HashMap.class,
        Json.type(String.class), valueType);
    final Supplier<Map<String, T>> empty = Suppliers.ofInstance(Collections.<String, T>emptyMap());

    this.snapshot = PersistentAtomicReference.create(snapshotFile, mapType, empty);
    this.reactor = new DefaultReactor("zk-ppcc:" + path, new Update(), REFRESH_INTERVAL_MILLIS);
    curator.getConnectionStateListenable().addListener(new ConnectionListener());
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
    reactor.startAsync().awaitRunning();
    reactor.signal();
  }

  @Override
  protected void shutDown() throws Exception {
    reactor.stopAsync().awaitTerminated();
  }

  public Map<String, T> getNodes() {
    return snapshot.get();
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

  private boolean isAlive() {
    return state().ordinal() < STOPPING.ordinal();
  }

  public interface Listener {

    void nodesChanged(PersistentPathChildrenCache<?> cache);

    void connectionStateChanged(ConnectionState state);
  }

  private class Update implements Reactor.Callback {

    final RetryIntervalPolicy retryIntervalPolicy = BoundedRandomExponentialBackoff.newBuilder()
        .setMinInterval(1, SECONDS)
        .setMaxInterval(30, SECONDS)
        .build();

    @Override
    public void run(final boolean timeout) throws InterruptedException {
      final RetryScheduler retryScheduler = retryIntervalPolicy.newScheduler();
      while (isAlive()) {
        try {
          update();
          return;
        } catch (Exception e) {
          // If an exception is thrown we must set the synced flag to false. Otherwise the next run
          // of update might not fetch data from zookeeper because it thinks everything is synced.
          synced = false;
          log.warn("update failed: {}", e.getMessage());
          Thread.sleep(retryScheduler.nextMillis());
        }
      }
    }
  }

  private void update() throws KeeperException, InterruptedException {
    log.debug("updating: {}", path);

    final Map<String, T> newSnapshot;
    final Map<String, T> currentSnapshot = snapshot.get();

    if (!synced) {
      synced = true;
      newSnapshot = sync();
    } else {
      newSnapshot = Maps.newHashMap(currentSnapshot);
    }

    // Fetch new data and register watchers for updated children
    final Iterator<String> iterator = changes.iterator();
    while (iterator.hasNext()) {
      final String child = iterator.next();
      iterator.remove();
      final String node = ZKPaths.makePath(path, child);
      log.debug("fetching change: {}", node);
      final T value;
      try {
        final byte[] bytes = curator.getData()
            .usingWatcher(dataWatcher)
            .forPath(node);
        value = Json.read(bytes, valueType);
      } catch (KeeperException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      newSnapshot.put(node, value);
    }

    if (!currentSnapshot.equals(newSnapshot)) {
      snapshot.setUnchecked(newSnapshot);
      fireNodesChanged();
    }
  }

  private void fireNodesChanged() {
    for (final Listener listener : listeners) {
      try {
        listener.nodesChanged(this);
      } catch (Exception e) {
        log.error("Listener threw exception", e);
      }
    }
  }

  /**
   * Fetch new snapshot and register watchers.
   */
  private Map<String, T> sync() throws KeeperException {
    log.debug("syncing: {}", path);

    final Map<String, T> newSnapshot = Maps.newHashMap();

    // Fetch new snapshot and register watchers
    try {
      final List<String> children = getChildren();
      log.debug("children: {}", children);
      for (final String child : children) {
        final String node = ZKPaths.makePath(path, child);
        final byte[] bytes = curator.getData()
            .usingWatcher(dataWatcher)
            .forPath(node);
        final String json = new String(bytes, UTF_8);
        log.debug("child: {}={}", node, json);
        final T value;
        try {
          value = Json.read(bytes, valueType);
        } catch (IOException e) {
          log.warn("failed to parse node: {}: {}", node, json, e);
          // Treat parse failure as absence
          continue;
        }
        newSnapshot.put(node, value);
      }
    } catch (KeeperException e) {
      throw e;
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }

    return newSnapshot;
  }

  private List<String> getChildren() throws Exception {
    final Stat childrenStat = new Stat();

    while (true) {
      final List<String> possibleChildren = curator.getChildren()
          .storingStatIn(childrenStat)
          .usingWatcher(childrenWatcher)
          .forPath(path);

      if (clusterId == null) {
        // Do not do any checks if the clusterId is not specified on the command line.
        return possibleChildren;
      }

      try {
        curator.inTransaction()
            .check().forPath(Paths.configId(clusterId)).and()
            .check().withVersion(childrenStat.getVersion()).forPath(path).and()
            .commit();
      } catch (KeeperException.BadVersionException e) {
        // Jobs have somehow changed while we were creating the transaction, retry.
        continue;
      }

      return possibleChildren;
    }
  }

  private class ChildrenWatcher implements CuratorWatcher {

    @Override
    public void process(final WatchedEvent event) throws Exception {
      log.debug("children event: {}", event);
      synced = false;
      reactor.signal();
    }
  }

  private class DataWatcher implements CuratorWatcher {

    @Override
    public void process(final WatchedEvent event) throws Exception {
      log.debug("data event: {}", event);
      if (event.getType() == NodeDataChanged) {
        final String child = ZKPaths.getNodeFromPath(event.getPath());
        changes.add(child);
        reactor.signal();
      }
    }
  }

  private class ConnectionListener implements ConnectionStateListener {

    @Override
    public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
      log.debug("connection state change: {}", newState);
      if (newState == ConnectionState.RECONNECTED) {
        synced = false;
        reactor.signal();
      }
      fireConnectionStateChanged(newState);
    }
  }
}
