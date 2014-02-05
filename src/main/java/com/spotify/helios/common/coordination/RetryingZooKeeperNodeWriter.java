/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.coordination;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

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

  private final Map<String, Write> front = Maps.newHashMap();
  private final Map<String, Write> back = Maps.newHashMap();
  private final Object lock = new Object() {};

  public RetryingZooKeeperNodeWriter(final ZooKeeperClient client) {
    this.client = client;
    this.reactor = new Reactor(new Update(), RETRY_INTERVAL_MILLIS);
  }

  public ListenableFuture<Void> set(final String path, final byte[] data) {
    final Write write = new Write(data);
    final Write prev;
    synchronized (lock) {
      prev = front.put(path, write);
    }
    reactor.update();
    if (prev != null) {
      prev.cancel(false);
    }
    return write;
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
        for (Map.Entry<String, Write> entry : front.entrySet()) {
          final Write prev = back.put(entry.getKey(), entry.getValue());
          if (prev != null) {
            prev.cancel(false);
          }
        }
        front.clear();
      }
      log.debug("writing: {}", back.keySet());
      for (Map.Entry<String, Write> entry : ImmutableMap.copyOf(back).entrySet()) {
        final String path = entry.getKey();
        final Write write = entry.getValue();
        try {
          if (client.stat(path) == null) {
            client.createAndSetData(path, write.data);
          } else {
            client.setData(path, write.data);
          }
          back.remove(path);
          write.done();
        } catch (KeeperException e) {
          log.error("Failed writing node: {}", path, e);
        }
      }
    }
  }

  private class Write extends AbstractFuture<Void> {

    final byte[] data;

    private Write(final byte[] data) {
      this.data = data;
    }

    public void done() {
      set(null);
    }
  }
}
