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

import com.codahale.metrics.health.HealthCheck;
import com.spotify.helios.servicescommon.RiemannFacade;
import io.dropwizard.lifecycle.Managed;
import java.sql.Connection;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperHealthChecker extends HealthCheck implements Managed {

  private final ZooKeeperClient zooKeeperClient;
  private final ConnectionStateListener connectionStateListener;
  private final RiemannFacade facade;

  private final ScheduledExecutorService scheduler;
  private final TimeUnit timeUnit;
  private final long interval;

  private AtomicReference<ConnectionState> connectionState = new AtomicReference<>();
  private AtomicReference<Instant> changedAt = new AtomicReference<>();

  public ZooKeeperHealthChecker(final ZooKeeperClient zooKeeperClient, final RiemannFacade facade,
                                final TimeUnit timeUnit, final long interval) {

    this.scheduler = Executors.newScheduledThreadPool(2);
    this.facade = facade.stack("zookeeper-connection");
    this.zooKeeperClient = zooKeeperClient;
    this.timeUnit = timeUnit;
    this.interval = interval;

    this.connectionStateListener = (client, newState) -> {
      final ConnectionState oldState = connectionState.getAndSet(newState);
      if (oldState != newState) {
        changedAt.set(Instant.now());
      }
    };
  }

  @Override
  public void start() throws Exception {
    zooKeeperClient.getConnectionStateListenable().addListener(connectionStateListener);
    scheduler.scheduleAtFixedRate(this::reportState, 0, interval, timeUnit);

    // call the listener in case Curator is already connected (as it won't fire a change then)
    fireInitialEvent();
  }

  private void fireInitialEvent() throws KeeperException {
    // the only Zookeeper.States value of interest is CONNECTED, the rest all signal a lack of
    // fully established connection
    final ZooKeeper.States state = zooKeeperClient.getState();
    final ConnectionState interpretedState =
        state == ZooKeeper.States.CONNECTED ? ConnectionState.CONNECTED : ConnectionState.LOST;

    connectionStateListener.stateChanged(zooKeeperClient.getCuratorFramework(), interpretedState);
  }

  @Override
  public void stop() throws Exception {
    // probably irrelevant but remove the listener to be safe
    zooKeeperClient.getConnectionStateListenable().removeListener(connectionStateListener);
    scheduler.shutdown();
  }

  @Override
  protected Result check() throws Exception {
    final ConnectionState state = this.connectionState.get();
    if (state != null && state.isConnected()) {
      return Result.healthy();
    } else {
      return Result.unhealthy(description());
    }
  }

  private String description() {
    final ConnectionState connectionState = this.connectionState.get();
    if (connectionState == null) {
      return "unknown ConnectionState";
    } else {
      return "connection state is " + connectionState + ", state changed at " + this.changedAt;
    }
  }

  private void reportState() {
    final ConnectionState connectionState = this.connectionState.get();
    if (connectionState == null) {
      return; // don't report anything until we get a known status
    }

    facade.event()
        .state(connectionState.isConnected() ? "ok" : "critical")
        .metric(connectionState.isConnected() ? 1.0 : 0.0)
        .ttl(timeUnit.toSeconds(interval * 3))
        .tags("zookeeper", "connection")
        .description(description())
        .send();
  }
}
