/*
 * Copyright (c) 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.servicescommon;

import com.spotify.helios.common.Hash;
import com.spotify.helios.servicescommon.coordination.Paths;

import org.apache.curator.framework.api.ACLProvider;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.bouncycastle.util.encoders.Base64;

import static org.apache.zookeeper.ZooDefs.Perms.CREATE;
import static org.apache.zookeeper.ZooDefs.Perms.READ;
import static org.apache.zookeeper.ZooDefs.Perms.WRITE;
import static org.apache.zookeeper.ZooDefs.Perms.DELETE;

/**
 * Contains a single factory method for creating ACLProvider used by Helios (agents and masters),
 * that controls the ACLs set on nodes when they are created.
 * <p>
 * If not authentication is provided a client has no access (not even read access). There are two
 * different roles: agents and masters. Masters are granted all permissions except ADMIN to all
 * nodes. Agents are only granted the permissions it needs, to a subset of all the nodes. This
 * limits the consequences of the agent credentials being compromised.
 * <p>
 * Currently all agents share the same permissions, using a single shared credential. I.e. an agent
 * can modify data that "belongs" to another agent (to the same extent that it can modify data that
 * belongs to it).
 * <p>
 * The provider uses ZooKeeper's "digest" ACL scheme.
 */
public class ZooKeeperAclProviders {

  private static final String PATH_COMPONENT_WILDCARD = "[^/]+";
  private static final String DIGEST_SCHEME = "digest";

  public static String digest(final String user, final String password) {
    return Base64.toBase64String(Hash.sha1digest(
        String.format("%s:%s", user, password).getBytes()));
  }

  public static ACLProvider heliosAclProvider(final String masterUser, final String masterDigest,
                                              final String agentUser, final String agentDigest) {
    final Id masterId = new Id(DIGEST_SCHEME, String.format("%s:%s", masterUser, masterDigest));
    final Id agentId = new Id(DIGEST_SCHEME, String.format("%s:%s", agentUser, agentDigest));

    return RuleBasedZooKeeperAclProvider.builder()
        // Set the default ACL to grant everyone CRWD permissions to master and READ to agents.
        // Note that the default ACLs should never be used though, as we set up catch-all rules
        // below.
        .defaultAcl(new ACL(CREATE | READ | WRITE | DELETE, masterId), new ACL(READ, agentId))
        // Master has CRWD permissions on all paths
        .rule(".*", CREATE | READ | WRITE | DELETE, masterId)
        // Agent has READ permission on all paths
        .rule(".*", READ, agentId)
        // The agent needs to create the /config/hosts/<host> path and nodes below it. It also needs
        // DELETE permissions since agents will delete it's "own" subtree when re-registering itself
        // (when a machine is reinstalled with the same name). Note that agent does however not have
        // any permissions, except read, to /config/hosts/<host>/jobs -- which means an agent can't
        // [typically] cause jobs to be deployed on other agents (this is not fool-proof -- if you
        // can predict an agent's name before it's created you inject data that causes it).
        .rule(Paths.configHosts(), CREATE | DELETE, agentId)
        .rule(Paths.configHost(PATH_COMPONENT_WILDCARD), CREATE | DELETE, agentId)
        .rule(Paths.configHostId(PATH_COMPONENT_WILDCARD), CREATE | DELETE, agentId)
        .rule(Paths.configHostPorts(PATH_COMPONENT_WILDCARD), CREATE | DELETE, agentId)
        .rule(Paths.statusHosts(), CREATE | DELETE, agentId)
        .rule(Paths.statusHost(PATH_COMPONENT_WILDCARD), CREATE | DELETE, agentId)
        .rule(Paths.statusHostJobs(PATH_COMPONENT_WILDCARD), CREATE | DELETE, agentId)
        .rule(Paths.statusHostJob(PATH_COMPONENT_WILDCARD, PATH_COMPONENT_WILDCARD), WRITE, agentId)
        .rule(Paths.statusHostAgentInfo(PATH_COMPONENT_WILDCARD), WRITE, agentId)
        .rule(Paths.statusHostLabels(PATH_COMPONENT_WILDCARD), WRITE, agentId)
        .rule(Paths.statusHostEnvVars(PATH_COMPONENT_WILDCARD), WRITE, agentId)
        .rule(Paths.statusHostUp(PATH_COMPONENT_WILDCARD), WRITE, agentId)
        // Grant agents CREATE permissions to the entire /history/jobs tree
        .rule(Paths.historyJobs() + "(/.+)?", CREATE, agentId)
        // Grant agents DELETE permissions to nodes under /history/jobs/<job>/hosts/<host>/events
        // -- needed for pruning old task history events
        .rule(Paths.historyJobHostEvents(
            PATH_COMPONENT_WILDCARD, PATH_COMPONENT_WILDCARD), DELETE, agentId)
        .build();
  }
}
