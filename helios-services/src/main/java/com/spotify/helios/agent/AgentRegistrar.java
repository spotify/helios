/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.SettableFuture;

import com.spotify.helios.servicescommon.DefaultReactor;
import com.spotify.helios.servicescommon.Reactor;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.google.common.base.Charsets.UTF_8;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode.EPHEMERAL;

public class AgentRegistrar extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(AgentRegistrar.class);

  private static final byte[] EMPTY_BYTES = new byte[]{};

  private final ZooKeeperClient client;
  private final String name;
  private final String id;

  private PersistentEphemeralNode upNode;

  private SettableFuture<Void> complete = SettableFuture.create();

  private final Reactor reactor = new DefaultReactor("agent-registrar", new Update());

  private ConnectionStateListener listener = new ConnectionStateListener() {
    @Override
    public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
      if (newState == ConnectionState.RECONNECTED) {
        reactor.signal();
      }
    }
  };


  public AgentRegistrar(final ZooKeeperClient client, final String name, final String id) {
    this.client = client;
    this.name = name;
    this.id = id;
  }

  public SettableFuture<Void> getCompletionFuture() {
    return complete;
  }

  @Override
  protected void startUp() throws Exception {
    client.getConnectionStateListenable().addListener(listener);
    reactor.startAsync().awaitRunning();
    reactor.signal();
  }

  @Override
  protected void shutDown() throws Exception {
    reactor.stopAsync().awaitTerminated();
    if (upNode != null) {
      try {
        upNode.close();
      } catch (IOException e) {
        log.warn("Exception on closing up node: {}", e.getMessage());
      }
    }
  }


  private void register() throws KeeperException {
    final String idPath = Paths.configHostId(name);

    final Stat stat = client.exists(idPath);
    if (stat == null) {
      // This would've been nice to do in a transaction but PathChildrenCache ensures paths
      // so we can't know what paths already exist so assembling a suitable transaction is too
      // painful.
      client.ensurePath(Paths.configHost(name));
      client.ensurePath(Paths.configHost(name));
      client.ensurePath(Paths.configHostJobs(name));
      client.ensurePath(Paths.configHostPorts(name));
      client.ensurePath(Paths.statusHost(name));
      client.ensurePath(Paths.statusHostJobs(name));

      // Finish registration by creating the id node last
      client.createAndSetData(idPath, id.getBytes(UTF_8));
    } else {
      final byte[] bytes = client.getData(idPath);
      final String existingId = bytes == null ? "" : new String(bytes, UTF_8);
      if (!id.equals(existingId)) {
        final String message = format("Another agent already registered as '%s' " +
                                      "(local=%s remote=%s).", name, id, existingId);
        complete.setException(new IllegalStateException(message));
        return;
      }
    }

    // Start the up node
    if (upNode == null) {
      final String upPath = Paths.statusHostUp(name);
      upNode = client.persistentEphemeralNode(upPath, EPHEMERAL, EMPTY_BYTES);
      upNode.start();
    }

    complete.set(null);
  }

  private class Update implements Reactor.Callback {

    final RetryIntervalPolicy RETRY_INTERVAL_POLICY = BoundedRandomExponentialBackoff.newBuilder()
        .setMinInterval(1, SECONDS)
        .setMaxInterval(30, SECONDS)
        .build();

    public void run() throws InterruptedException {
      final RetryScheduler retryScheduler = RETRY_INTERVAL_POLICY.newScheduler();
      while (isRunning()) {
        try {
          register();
          return;
        } catch (KeeperException e) {
          final long sleep = retryScheduler.nextMillis();
          log.error("Agent registration failed, retrying in {} ms", sleep, e);
          Thread.sleep(sleep);
        }
      }
    }
  }
}
