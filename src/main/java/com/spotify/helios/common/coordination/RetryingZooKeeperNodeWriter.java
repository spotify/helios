/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.coordination;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.spotify.helios.common.Reactor;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A ZooKeeper node writer that retries forever.
 */
public class RetryingZooKeeperNodeWriter {

  private static final Logger log = LoggerFactory.getLogger(RetryingZooKeeperNodeWriter.class);

  private static final long RETRY_INTERVAL_MILLIS = 5000;

  private final ZooKeeperClient client;

  private final Reactor reactor;

  private final Map<String, byte[]> front = Maps.newHashMap();
  private final Map<String, byte[]> back = Maps.newHashMap();
  private final Object lock = new Object() {};

  public RetryingZooKeeperNodeWriter(final ZooKeeperClient client) {
    this.client = client;
    this.reactor = new Reactor(new Update(), RETRY_INTERVAL_MILLIS);
  }

  public void set(final String path, final byte[] data) {
    synchronized (lock) {
      front.put(path, data);
    }
    reactor.update();
  }

  public void close() throws InterruptedException {
    reactor.close();
  }

  private class Update implements Runnable {

    @Override
    public void run() {
      if (front.isEmpty() && back.isEmpty()) {
        return;
      }
      synchronized (lock) {
        back.putAll(front);
        front.clear();
      }
      log.debug("writing: {}", back.keySet());
      for (Map.Entry<String, byte[]> entry : ImmutableMap.copyOf(back).entrySet()) {
        final String path = entry.getKey();
        final byte[] data = entry.getValue();
        try {
          if (client.stat(path) == null) {
            client.createAndSetData(path, data);
          } else {
            client.setData(path, data);
          }
          back.remove(path);
        } catch (KeeperException e) {
          log.error("Failed writing node: {}", path, e);
        }
      }
    }
  }
}
