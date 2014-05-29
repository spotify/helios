/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.servicescommon;

import com.google.common.util.concurrent.AbstractIdleService;

import com.spotify.helios.agent.BoundedRandomExponentialBackoff;
import com.spotify.helios.agent.RetryIntervalPolicy;
import com.spotify.helios.agent.RetryScheduler;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.util.concurrent.Service.State.STOPPING;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ZooKeeperRegistrar extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperRegistrar.class);

  private final ZooKeeperClient client;

  private final Reactor reactor;
  private final ZooKeeperRegistrarEventListener eventListener;

  private final RetryIntervalPolicy retryIntervalPolicy;

  private ConnectionStateListener listener = new ConnectionStateListener() {
    @Override
    public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
      if (newState == ConnectionState.RECONNECTED) {
        reactor.signal();
      }
    }
  };

  public ZooKeeperRegistrar(final ZooKeeperClient client,
                            final ZooKeeperRegistrarEventListener eventListener) {
    this(client, eventListener, BoundedRandomExponentialBackoff.newBuilder()
        .setMinInterval(1, SECONDS)
        .setMaxInterval(30, SECONDS)
        .build());
  }

  public ZooKeeperRegistrar(final ZooKeeperClient client,
                            final ZooKeeperRegistrarEventListener eventListener,
                            final RetryIntervalPolicy retryIntervalPolicy) {
    this.client = client;
    this.eventListener = eventListener;
    this.retryIntervalPolicy = retryIntervalPolicy;
    this.reactor = new DefaultReactor("zk-client-async-init", new Update());
  }

  @Override
  protected void startUp() throws Exception {
    eventListener.startUp();
    client.getConnectionStateListenable().addListener(listener);
    reactor.startAsync().awaitRunning();
    reactor.signal();
  }

  @Override
  protected void shutDown() throws Exception {
    reactor.stopAsync().awaitTerminated();
    eventListener.shutDown();
  }

  private class Update implements Reactor.Callback {

    @Override
    public void run(final boolean timeout) throws InterruptedException {
      final RetryScheduler retryScheduler = retryIntervalPolicy.newScheduler();
      while (isAlive()) {
        try {
          eventListener.tryToRegister(client);
          return;
        } catch (KeeperException e) {
          final long sleep = retryScheduler.nextMillis();
          log.error("ZooKeeper registration failed, retrying in {} ms", sleep, e);
          Thread.sleep(sleep);
        }
      }
    }
  }

  private boolean isAlive() {
    return state().ordinal() < STOPPING.ordinal();
  }
}
