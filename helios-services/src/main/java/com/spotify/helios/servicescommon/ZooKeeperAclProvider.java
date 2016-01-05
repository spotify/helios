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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.spotify.helios.servicescommon.coordination.Paths;

import org.apache.curator.framework.api.ACLProvider;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.zookeeper.ZooDefs.Perms.CREATE;
import static org.apache.zookeeper.ZooDefs.Perms.READ;
import static org.apache.zookeeper.ZooDefs.Perms.WRITE;
import static org.apache.zookeeper.ZooDefs.Perms.DELETE;

/**
 * A ZooKeeper ACLProvider for Helios that controls the ACLs set on nodes when they are created.
 * It allows read-only access for unauthenticated users. To gain more permissions you need to
 * provide credentials. There are two different roles: agents and masters. Masters are granted
 * all permissions (except ADMIN) to all nodes. Agents are only granted the permissions it needs, to
 * a subset of all the nodes. This limits the consequences of the agent credentials being
 * compromised.
 * <p>
 * Currently all agents share the same permissions, using a single shared credential. I.e. an agent
 * can modify data that "belongs" to another agent (to the same extent that it can modify data that
 * belongs to it).
 * <p>
 * Internally, the provider is implemented by granting permissions for users on paths based on
 * regular expressions. The permissions for all rules that match a path apply. For example if there
 * are two rules:
 * <ol>
 *   <li>"/foo.*", DELETE, user1</li>
 *   <li>"/foo/bar", CREATE, user1</li>
 * </ol>
 * Then user1 can create and delete nodes on the /foo/bar node, but he can not delete nodes under
 * the /foo node.
 * <p>
 * The provider uses ZooKeeper's "digest" ACL scheme.
 */
public class ZooKeeperAclProvider implements ACLProvider {

  private static final String WILDCARD = "[^/]+";
  private static final String DIGEST_SCHEME = "digest";
  public static final String MASTER_USER = "helios-master";
  public static final String AGENT_USER = "helios-agent";

  private final ImmutableList<ACL> defaultAcl;
  private final List<Rule> rules = Lists.newArrayList();

  private static class Rule {

    private final Pattern pattern;
    private final Id id;
    private final int perms;

    Rule(final String regex, final int perms, final Id id) {
      this.pattern = Pattern.compile(regex);
      this.perms = perms;
      this.id = id;
    }

    boolean matches(final String path) {
      return pattern.matcher(path).matches();
    }
  }

  public ZooKeeperAclProvider(final String masterDigest, final String agentDigest) {
    final Id masterId = new Id(DIGEST_SCHEME, String.format("%s:%s", MASTER_USER, masterDigest));
    final Id agentId = new Id(DIGEST_SCHEME, String.format("%s:%s", AGENT_USER, agentDigest));
    // By default, give everyone READ-only access
    // Note that the default ACL should never be used, since we set up catch-all rules below
    this.defaultAcl = ImmutableList.of(new ACL(READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));

    // Master has CRWD permissions on all paths
    rule(".*", CREATE | READ | WRITE | DELETE, masterId);
    // Agent has READ permission on all paths
    rule(".*", READ, agentId);
    // Grant everyone READ-only access to make troubleshooting easier
    rule(".*", READ, ZooDefs.Ids.ANYONE_ID_UNSAFE);

    // The agent needs to be create the /config/hosts/<host> path and nodes below it. It also needs
    // DELETE permissions since agents will delete it's "own" node when re-registering itself when
    // a machine is reinstalled. Note that agent does however not have any permissions, except read,
    // to /config/hosts/<host>/jobs -- which means an agent can't [typically] cause jobs to be
    // deployed on other agents.
    rule(Paths.configHosts(), CREATE | DELETE, agentId);
    rule(Paths.configHost(WILDCARD), CREATE | DELETE, agentId);
    rule(Paths.configHostId(WILDCARD), CREATE | DELETE, agentId);
    rule(Paths.configHostPorts(WILDCARD), CREATE | DELETE, agentId);

    rule(Paths.statusHosts(), CREATE | DELETE, agentId);
    rule(Paths.statusHost(WILDCARD), CREATE | DELETE, agentId);
    rule(Paths.statusHostJobs(WILDCARD), CREATE | DELETE, agentId);
    rule(Paths.statusHostJob(WILDCARD, WILDCARD), WRITE, agentId);
    rule(Paths.statusHostAgentInfo(WILDCARD), WRITE, agentId);
    rule(Paths.statusHostLabels(WILDCARD), WRITE, agentId);
    rule(Paths.statusHostEnvVars(WILDCARD), WRITE, agentId);
    rule(Paths.statusHostUp(WILDCARD), WRITE, agentId);

    // Grant agents CREATE permissions to the entire /history/jobs tree
    rule(Paths.historyJobs() + "(/.+)?", CREATE, agentId);
    // Grant agents DELETE permissions to nodes under /history/jobs/<job>/hosts/<host>/events
    // -- needed for pruning old task history events
    rule(Paths.historyJobHostEvents(WILDCARD, WILDCARD), DELETE, agentId);
  }

  private void rule(final String pathRegex, final int permissions, final Id id) {
    rules.add(new Rule(pathRegex, permissions, id));
  }

  @Override
  public List<ACL> getDefaultAcl() {
    return defaultAcl;
  }

  @Override
  public List<ACL> getAclForPath(final String path) {
    // id -> permissions
    final Map<Id, Integer> matching = Maps.newHashMap();

    for (final Rule rule : rules) {
      if (rule.matches(path)) {
        final int existingPerms = matching.containsKey(rule.id) ? matching.get(rule.id) : 0;
        matching.put(rule.id, rule.perms | existingPerms);
      }
    }

    if (matching.isEmpty()) {
      return null;
    }

    final List<ACL> acls = Lists.newArrayList();
    for (final Map.Entry<Id, Integer> e : matching.entrySet()) {
      acls.add(new ACL(e.getValue(), e.getKey()));
    }

    return acls;
  }
}
