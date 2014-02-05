/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.common.coordination;

import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.common.PersistentAtomicReference;
import com.spotify.helios.common.Reactor;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.MapDifference.ValueDifference;

/**
 * A map that persists modification locally on disk and attempt to replicate modifications to
 * ZooKeeper, retrying forever until successful. Note that ZooKeeper is only written to and never
 * read from, so this is not a distributed map. Multiple changes to the same key are folded and only
 * the last value is written to ZooKeeper.
 */
public class ZooKeeperUpdatingPersistentMap extends AbstractMap<String, byte[]> {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperUpdatingPersistentMap.class);

  private static final long RETRY_INTERVAL = 5000;

  private static final Map<String, byte[]> EMPTY_ENTRIES = Collections.emptyMap();
  private static final TypeReference<Map<String, byte[]>> ENTRIES_TYPE =
      new TypeReference<Map<String, byte[]>>() {};

  private final ZooKeeperClient client;
  private final Reactor reactor;
  private final PersistentAtomicReference<Map<String, byte[]>> entries;
  private final PersistentAtomicReference<Map<String, byte[]>> written;

  private final Object lock = new Object() {};

  private ZooKeeperUpdatingPersistentMap(final ZooKeeperClient client, final Path stateFile)
      throws IOException {
    this.client = client;
    this.entries = PersistentAtomicReference.create(stateFile, ENTRIES_TYPE,
                                                    Suppliers.ofInstance(EMPTY_ENTRIES));
    this.written = PersistentAtomicReference.create(stateFile.toString() + ".written", ENTRIES_TYPE,
                                                    Suppliers.ofInstance(EMPTY_ENTRIES));
    this.reactor = new Reactor(new Update(), RETRY_INTERVAL);
  }

  @Override
  public byte[] put(final String key, final byte[] value) {
    synchronized (lock) {
      final Map<String, byte[]> mutable = Maps.newHashMap(entries.get());
      final byte[] prev = mutable.put(key, value);
      try {
        entries.set(ImmutableMap.copyOf(mutable));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
      reactor.update();
      return prev;
    }
  }

  @Override
  public byte[] remove(final Object key) {
    if (!(key instanceof String)) {
      return null;
    }
    synchronized (lock) {
      final Map<String, byte[]> mutable = Maps.newHashMap(entries.get());
      final byte[] value = mutable.remove(key);
      try {
        entries.set(ImmutableMap.copyOf(mutable));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
      reactor.update();
      return value;
    }
  }

  @Override
  public byte[] get(final Object key) {
    return entries.get().get(key);
  }

  @Override
  public Set<Entry<String, byte[]>> entrySet() {
    return entries.get().entrySet();
  }

  public static ZooKeeperUpdatingPersistentMap create(final ZooKeeperClient client,
                                                      final Path stateFile) throws IOException {
    return new ZooKeeperUpdatingPersistentMap(client, stateFile);
  }

  public void close() throws InterruptedException {
    reactor.close();
  }

  private class Update implements Runnable {

    @Override
    public void run() {
      final MapDifference<String, byte[]> difference =
          Maps.difference(entries.get(), written.get());
      if (difference.areEqual()) {
        return;
      }

      final Map<String, byte[]> newWritten = Maps.newHashMap(written.get());

      final Map<String, byte[]> create = difference.entriesOnlyOnLeft();
      final Map<String, ValueDifference<byte[]>> update = difference.entriesDiffering();
      final Map<String, byte[]> delete = difference.entriesOnlyOnRight();

      log.debug("create: {}", create.keySet());
      log.debug("update: {}", update.keySet());
      log.debug("delete: {}", delete.keySet());

      for (final Entry<String, byte[]> entry : create.entrySet()) {
        write(entry.getKey(), entry.getValue());
        newWritten.put(entry.getKey(), entry.getValue());
      }

      for (final Entry<String, ValueDifference<byte[]>> entry : update.entrySet()) {
        write(entry.getKey(), entry.getValue().leftValue());
        newWritten.put(entry.getKey(), entry.getValue().leftValue());
      }

      for (final Entry<String, byte[]> entry : delete.entrySet()) {
        delete(entry.getKey());
        newWritten.remove(entry.getKey());
      }

      try {
        written.set(ImmutableMap.copyOf(newWritten));
      } catch (IOException e) {
        log.error("Failed to write state: {}", written);
        throw Throwables.propagate(e);
      }
    }

    private void delete(final String path) {
      try {
        if (client.stat(path) != null) {
          client.delete(path);
        }
      } catch (KeeperException e) {
        log.error("Failed to delete node: {}", path, e);
        throw Throwables.propagate(e);
      }
    }

    private void write(final String path, final byte[] data) {
      try {
        if (client.stat(path) != null) {
          client.setData(path, data);
        } else {
          client.createAndSetData(path, data);
        }
      } catch (KeeperException e) {
        log.error("Failed to write node: {}", path, e);
        throw Throwables.propagate(e);
      }
    }
  }
}
