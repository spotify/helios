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

package com.spotify.helios.agent;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.fromNullable;
import static java.lang.String.format;
import static org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode.EPHEMERAL;

import com.spotify.helios.common.Clock;
import com.spotify.helios.master.HostNotFoundException;
import com.spotify.helios.servicescommon.ZooKeeperRegistrar;
import com.spotify.helios.servicescommon.ZooKeeperRegistrarUtil;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persistently tries to register with ZK in the face of a ZK outage.
 */
public class AgentZooKeeperRegistrar implements ZooKeeperRegistrar {

  private static final Logger log = LoggerFactory.getLogger(AgentZooKeeperRegistrar.class);

  private static final byte[] EMPTY_BYTES = new byte[]{};

  private final String name;
  private final String id;
  private final long zooKeeperRegistrationTtlMillis;
  private Clock clock;

  private PersistentEphemeralNode upNode;

  public AgentZooKeeperRegistrar(final String name, final String id,
                                 final int zooKeeperRegistrationTtlMinutes, final Clock clock) {
    this.name = name;
    this.id = id;
    this.zooKeeperRegistrationTtlMillis =
        TimeUnit.MILLISECONDS.convert(zooKeeperRegistrationTtlMinutes, TimeUnit.MINUTES);
    this.clock = clock;
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
        final Throwable cause = fromNullable(e.getCause()).or(e);
        log.warn("Exception on closing up node: {}", cause);
      }
    }
  }

  @Override
  public boolean tryToRegister(ZooKeeperClient client)
      throws KeeperException, HostNotFoundException {
    final String idPath = Paths.configHostId(name);
    final String hostInfoPath = Paths.statusHostInfo(name);

    final Stat stat = client.exists(idPath);
    if (stat == null) {
      log.debug("Agent id node not present, registering agent {}: {}", id, name);
      ZooKeeperRegistrarUtil.registerHost(client, idPath, name, id);
    } else {
      final byte[] bytes = client.getData(idPath);
      final String existingId = bytes == null ? "" : new String(bytes, UTF_8);
      if (!id.equals(existingId)) {
        final Stat hostInfoStat = client.stat(hostInfoPath);
        if (hostInfoStat != null) {
          final long mtime = hostInfoStat.getMtime();
          final long diff = clock.now().getMillis() - mtime;
          if (diff < zooKeeperRegistrationTtlMillis) {
            final String message = format(
                "Another agent already registered as '%s' (local=%s remote=%s). "
                + "That agent's registration expires in %d seconds",
                name, id.trim(), existingId.trim(),
                TimeUnit.MILLISECONDS.toSeconds(zooKeeperRegistrationTtlMillis - diff));
            log.warn(message);
            return false;
          }

          log.info("Another agent has already registered as '{}', but its ID node was last "
                   + "updated more than {} milliseconds ago. I\'m deregistering the agent with the"
                   + "old ID of {} and replacing it with this new agent with ID '{}'.",
              name, zooKeeperRegistrationTtlMillis, existingId.trim(), id.trim());
        } else {
          log.info("Another agent has registered as '{}', but it never updated '{}' in ZooKeeper. "
                   + "I'll assume it's dead and deregister it.", name, hostInfoPath);
        }

        ZooKeeperRegistrarUtil.reRegisterHost(client, name, id);
      } else {
        log.info("Matching agent id node already present, not registering agent {}: {}", id, name);
      }
    }

    // Start the up node
    if (upNode == null) {
      final String upPath = Paths.statusHostUp(name);
      log.debug("Creating up node: {}", upPath);
      client.ensurePath(upPath, true);
      upNode = client.persistentEphemeralNode(upPath, EPHEMERAL, EMPTY_BYTES);
      upNode.start();
    }

    log.info("ZooKeeper registration complete");
    return true;
  }

}
