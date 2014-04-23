/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.util.concurrent.SettableFuture;
import com.spotify.helios.servicescommon.ZooKeeperClientAsyncInitializer;
import com.spotify.helios.servicescommon.ZooKeeperClientConnectListener;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.google.common.base.Charsets.UTF_8;
import static java.lang.String.format;
import static org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode.EPHEMERAL;

public class AgentRegistrar extends ZooKeeperClientAsyncInitializer implements ZooKeeperClientConnectListener {

  private static final Logger log = LoggerFactory.getLogger(AgentRegistrar.class);

  private static final byte[] EMPTY_BYTES = new byte[]{};

  private final String name;
  private final String id;

  private PersistentEphemeralNode upNode;

  public AgentRegistrar(final ZooKeeperClient client, final String name, final String id) {
    super(client);

    this.name = name;
    this.id = id;

    setCompleteFuture(SettableFuture.<Void>create());
  }

  @Override
  protected void shutDown() throws Exception {
    super.shutDown();

    if (upNode != null) {
      try {
        upNode.close();
      } catch (IOException e) {
        log.warn("Exception on closing up node: {}", e.getMessage());
      }
    }
  }


  @Override
  public void onConnect(SettableFuture<Void> complete) throws KeeperException {
    final String idPath = Paths.configHostId(name);
    final ZooKeeperClient client = getZKClient();

    final Stat stat = client.exists(idPath);
    if (stat == null) {
      log.debug("Agent id node not present, registering agent {}: {}", id, name);

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
        log.debug(message);
        complete.setException(new IllegalStateException(message));
        return;
      } else {
        log.debug("Matching agent id node already present, not registering agent {}: {}", id, name);
      }
    }

    // Start the up node
    if (upNode == null) {
      final String upPath = Paths.statusHostUp(name);
      log.debug("Creating up node: {}", upPath);
      upNode = client.persistentEphemeralNode(upPath, EPHEMERAL, EMPTY_BYTES);
      upNode.start();
    }

    complete.set(null);
  }

}
