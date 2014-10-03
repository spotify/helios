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

import com.spotify.helios.servicescommon.RiemannFacade;
import io.dropwizard.lifecycle.Managed;
import com.codahale.metrics.health.HealthCheck;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ZooKeeperHealthChecker extends HealthCheck
    implements Managed, PathChildrenCacheListener, Runnable {
  private static final String UNKNOWN = "UNKNOWN";

  private final ScheduledExecutorService scheduler;
  private final PathChildrenCache cache;
  private final RiemannFacade facade;
  private final TimeUnit timeUnit;
  private final long interval;

  private AtomicReference<String> reasonString = new AtomicReference<String>(UNKNOWN);

  public ZooKeeperHealthChecker(final ZooKeeperClient zooKeeperClient, final String path,
                                final RiemannFacade facade, final TimeUnit timeUnit,
                                final long interval) {
    super();
    this.scheduler = Executors.newScheduledThreadPool(2);
    this.cache = new PathChildrenCache(zooKeeperClient.getCuratorFramework(), path, true, false,
        scheduler);
    this.facade = facade.stack("zookeeper-connection");
    this.timeUnit = timeUnit;
    this.interval = interval;

    cache.getListenable().addListener(this);
  }

  @Override
  public void run() {
    String reason = reasonString.get();
    if (UNKNOWN.equals(reasonString.get())) {
      return; // don't report anything until we get a known status
    }

    if (reason != null) {
      facade.event()
          .state("critical")
          .metric(0.0)
          .ttl(timeUnit.toSeconds(interval * 3))
          .tags("zookeeper", "connection")
          .description(reason)
          .send();
    } else {
      facade.event()
          .state("ok")
          .metric(1.0)
          .tags("zookeeper", "connection")
          .ttl(timeUnit.toSeconds(interval * 3))
          .send();
    }
  }

  private void setState(String newState) {
    if ((reasonString.get() == null) != (newState == null)) {
      reasonString.set(newState);
      run();
    }
  }

  @Override
  public void childEvent(CuratorFramework curator, PathChildrenCacheEvent event)
      throws Exception {
    switch (event.getType()) {
      case INITIALIZED:
      case CONNECTION_RECONNECTED:
      case CHILD_ADDED:
      case CHILD_REMOVED:
      case CHILD_UPDATED:
        // If we get any of these, clearly we're connected.
        setState(null);
        break;

      case CONNECTION_LOST:
        setState("CONNECTION_LOST");
        break;
      case CONNECTION_SUSPENDED:
        setState("CONNECTION_SUSPENDED");
        break;
    }
  }

  @Override
  public void start() throws Exception {
    cache.start();

    scheduler.scheduleAtFixedRate(this, 0, interval, timeUnit);
  }

  @Override
  public void stop() throws Exception {
    scheduler.shutdownNow();
  }

  @Override
  protected Result check() throws Exception {
    if (reasonString.get() == null) {
      return Result.healthy();
    } else {
      return Result.unhealthy(reasonString.get());
    }
  }
}
