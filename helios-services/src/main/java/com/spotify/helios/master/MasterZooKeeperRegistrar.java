package com.spotify.helios.master;

import com.spotify.helios.servicescommon.ZooKeeperRegistrarEventListener;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;

import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MasterZooKeeperRegistrar implements ZooKeeperRegistrarEventListener {

  private static final Logger log = LoggerFactory.getLogger(MasterZooKeeperRegistrar.class);

  private final String name;
  private PersistentEphemeralNode upNode;

  public MasterZooKeeperRegistrar(String name) {
    this.name = name;
  }

  @Override
  public void startUp() throws Exception {

  }

  @Override
  public void shutDown() throws Exception {
    if (upNode != null) {
      try {
        upNode.close();
      } catch (IOException e) {
        log.warn("Exception on closing up node: {}", e.getMessage());
      }
    }
  }

  @Override
  public void tryToRegister(final ZooKeeperClient client) throws KeeperException {

    client.ensurePath(Paths.configHosts());
    client.ensurePath(Paths.configJobs());
    client.ensurePath(Paths.configJobRefs());
    client.ensurePath(Paths.statusHosts());
    client.ensurePath(Paths.statusMasters());
    client.ensurePath(Paths.historyJobs());

    if (upNode == null) {
      final String upPath = Paths.statusMasterUp(name);
      upNode = client.persistentEphemeralNode(upPath, PersistentEphemeralNode.Mode.EPHEMERAL, new byte[]{});
      upNode.start();
    }

    log.info("ZooKeeper registration complete");
  }
}
