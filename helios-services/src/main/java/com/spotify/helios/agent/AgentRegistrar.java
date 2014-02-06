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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;
import static java.lang.String.format;

public class AgentRegistrar extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(AgentRegistrar.class);

  private static final long RETRY_INTERVAL_MILLIS = 5000;

  private final ZooKeeperClient client;
  private final String name;
  private final String id;

  private SettableFuture<Void> complete = SettableFuture.create();

  private final Reactor reactor = new DefaultReactor("agent-registrar", new Update(),
                                                     RETRY_INTERVAL_MILLIS);

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
    reactor.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    reactor.stopAsync().awaitTerminated();
  }

  private class Update implements Reactor.Callback {

    @Override
    public void run() {
      try {
        final String idPath = Paths.configAgentId(name);

        final Stat stat = client.exists(idPath);
        if (stat == null) {
          // This would've been nice to do in a transaction but PathChildrenCache ensures paths
          // so we can't know what paths already exist so assembling a suitable transaction is too
          // painful.
          client.ensurePath(Paths.configAgent(name));
          client.ensurePath(Paths.configAgent(name));
          client.ensurePath(Paths.configAgentJobs(name));
          client.ensurePath(Paths.configAgentPorts(name));
          client.ensurePath(Paths.statusAgent(name));
          client.ensurePath(Paths.statusAgentJobs(name));

          // Finish registration by creating the id node last
          client.createAndSetData(idPath, id.getBytes(UTF_8));
        } else {
          final byte[] bytes = client.getData(idPath);
          final String existingId = bytes == null ? "" : new String(bytes, UTF_8);
          if (!id.equals(existingId)) {
            final String message = format("Another agent already registered as '%s' " +
                                          "(local=%s remote=%s).", name, id, existingId);
            complete.setException(new IllegalStateException(message));
            reactor.stopAsync();
          }
        }

        complete.set(null);
        reactor.stopAsync();
      } catch (KeeperException e) {
        log.error("Agent registration failed", e);
      }
    }
  }
}
