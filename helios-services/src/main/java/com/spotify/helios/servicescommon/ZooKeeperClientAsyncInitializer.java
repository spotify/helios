/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.servicescommon;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.SettableFuture;
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
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ZooKeeperClientAsyncInitializer extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperClientAsyncInitializer.class);

  private final ZooKeeperClient client;

  private SettableFuture<Void> complete;

  private final Reactor reactor;
  private ZooKeeperClientConnectListener clientConnectListener;

  private final long minIntervalMillis; // retry backoff interval
  private final long maxIntervalMillis; // retry backoff interval

  private ConnectionStateListener listener = new ConnectionStateListener() {
    @Override
    public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
      if (newState == ConnectionState.RECONNECTED) {
        reactor.signal();
      }
    }
  };

  public ZooKeeperClientAsyncInitializer(final ZooKeeperClient client) {
    this(client, 1 * 1000, 30 * 1000);
  }

  public ZooKeeperClientAsyncInitializer(final ZooKeeperClient client,
                                         long minIntervalMillis,
                                         long maxIntervalMillis) {
    this.client             = client;
    this.minIntervalMillis  = minIntervalMillis;
    this.maxIntervalMillis  = maxIntervalMillis;

    this.reactor = new DefaultReactor("zk-client-async-init", new Update());
  }

  public void setCompleteFuture(SettableFuture<Void> complete) { this.complete = complete; }
  public SettableFuture<Void> getCompletionFuture() { return complete; }

  protected ZooKeeperClient getZKClient() { return client; }

  @Override
  protected void startUp() throws Exception {
    client.getConnectionStateListenable().addListener(listener);
    reactor.startAsync().awaitRunning();
    reactor.signal();
  }

  @Override
  protected void shutDown() throws Exception {
    reactor.stopAsync().awaitTerminated();
  }

  public void setListener(ZooKeeperClientConnectListener zooKeeperClientConnectListener) {
    this.clientConnectListener = zooKeeperClientConnectListener;
  }

  private class Update implements Reactor.Callback {

    final RetryIntervalPolicy RETRY_INTERVAL_POLICY = BoundedRandomExponentialBackoff.newBuilder()
        .setMinInterval(minIntervalMillis, MILLISECONDS)
        .setMaxInterval(maxIntervalMillis, MILLISECONDS)
        .build();

    public void run() throws InterruptedException {
      final RetryScheduler retryScheduler = RETRY_INTERVAL_POLICY.newScheduler();
      while (isAlive()) {
        try {
          clientConnectListener.onConnect(complete);
          return;
        } catch (KeeperException e) {
          final long sleep = retryScheduler.nextMillis();
          log.error("Agent registration failed, retrying in {} ms", sleep, e);
          Thread.sleep(sleep);
        }
      }
    }
  }

  private boolean isAlive() {
    return state().ordinal() < STOPPING.ordinal();
  }
}
