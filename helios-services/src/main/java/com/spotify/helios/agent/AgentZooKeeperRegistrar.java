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

package com.spotify.helios.agent;

import com.google.common.util.concurrent.Service;

import com.spotify.helios.master.HostNotFoundException;
import com.spotify.helios.master.HostStillInUseException;
import com.spotify.helios.servicescommon.ZooKeeperRegistrarEventListener;
import com.spotify.helios.servicescommon.ZooKeeperRegistrarUtil;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;

import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.fromNullable;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode.EPHEMERAL;

/**
 * Persistently tries to register with ZK in the face of a ZK outage.
 */
public class AgentZooKeeperRegistrar implements ZooKeeperRegistrarEventListener {

  private static final Logger log = LoggerFactory.getLogger(AgentZooKeeperRegistrar.class);

  private static final byte[] EMPTY_BYTES = new byte[]{};
  private static final int AGENT_REGISTRATION_TTL_MINUTES = 10;
  private static final long AGENT_REGISTRATION_TTL_MILLIS =
      TimeUnit.MILLISECONDS.convert(AGENT_REGISTRATION_TTL_MINUTES, TimeUnit.MINUTES);

  private final Service agentService;
  private final String name;
  private final String id;

  private PersistentEphemeralNode upNode;

  public AgentZooKeeperRegistrar(final Service agentService, final String name, final String id) {
    this.agentService = agentService;
    this.name = name;
    this.id = id;
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
  public void tryToRegister(ZooKeeperClient client)
      throws KeeperException, HostStillInUseException, HostNotFoundException {
    final String idPath = Paths.configHostId(name);

    final Stat stat = client.exists(idPath);
    if (stat == null) {
      log.debug("Agent id node not present, registering agent {}: {}", id, name);
      ZooKeeperRegistrarUtil.registerHost(client, idPath, name, id);
    } else {
      final byte[] bytes = client.getData(idPath);
      final String existingId = bytes == null ? "" : new String(bytes, UTF_8);
      if (!id.equals(existingId)) {
        final long mtime = client.stat(idPath).getMtime();

        if ((currentTimeMillis() - mtime) > AGENT_REGISTRATION_TTL_MILLIS) {
          log.info("Another agent has already registered as '{}', but its ID node was last " +
                   "updated more than {} minutes ago. I\'m deregistering the agent with the old " +
                   "ID of {} and replacing it with this new agent with ID '{}'.",
                   name, AGENT_REGISTRATION_TTL_MINUTES, existingId, id);
          ZooKeeperRegistrarUtil.deregisterHost(client, name);
          ZooKeeperRegistrarUtil.registerHost(client, idPath, name, id);
        } else {
          final String message = format("Another agent already registered as '%s' " +
                                        "(local=%s remote=%s).", name, id, existingId);
          log.error(message);
          agentService.stopAsync();
          return;
        }
      } else {
        log.info("Matching agent id node already present, not registering agent {}: {}", id, name);
      }
    }

    // Start the up node
    if (upNode == null) {
      final String upPath = Paths.statusHostUp(name);
      log.debug("Creating up node: {}", upPath);
      upNode = client.persistentEphemeralNode(upPath, EPHEMERAL, EMPTY_BYTES);
      upNode.start();
    }

    log.info("ZooKeeper registration complete");
  }

}
