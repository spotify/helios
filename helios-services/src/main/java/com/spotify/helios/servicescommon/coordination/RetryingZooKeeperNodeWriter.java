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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.servicescommon.DefaultReactor;
import com.spotify.helios.servicescommon.Reactor;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A ZooKeeper node writer that retries forever.
 */
public class RetryingZooKeeperNodeWriter extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(RetryingZooKeeperNodeWriter.class);

  private static final long RETRY_INTERVAL_MILLIS = 5000;

  private final ZooKeeperClient client;

  private final Reactor reactor;

  private final Map<String, Write> front = Maps.newHashMap();
  private final Map<String, Write> back = Maps.newHashMap();
  private final Object lock = new Object() {};

  public RetryingZooKeeperNodeWriter(final String name, final ZooKeeperClient client) {
    this.client = client;
    this.reactor = new DefaultReactor(name, new Update(), RETRY_INTERVAL_MILLIS);
  }

  public ListenableFuture<Void> set(final String path, final byte[] data) {
    final Write write = new Write(data);
    final Write prev;
    synchronized (lock) {
      prev = front.put(path, write);
    }
    reactor.signal();
    if (prev != null) {
      prev.cancel(false);
    }
    return write;
  }

  @Override
  protected void startUp() throws Exception {
    reactor.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    reactor.stopAsync().awaitTerminated();
  }

  private class Update implements Reactor.Callback {

    @Override
    public void run(final boolean timeout) {
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

  private static class Write extends AbstractFuture<Void> {

    final byte[] data;

    private Write(final byte[] data) {
      this.data = data;
    }

    public void done() {
      set(null);
    }
  }
}
