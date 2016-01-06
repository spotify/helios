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

import org.apache.curator.framework.api.ACLProvider;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A ZooKeeper ACLProvider that uses regular expression-based rules to determine what ACLs govern
 * created nodes.
 * <p>
 * The permissions for all rules that match a path apply. For example if there
 * are two rules:
 * <ol>
 *   <li>"/foo.*", DELETE, usr1</li>
 *   <li>"/foo/bar", CREATE, user1</li>
 * </ol>
 * Then ACLs will be set so that user1 can create and delete nodes on the /foo/bar node, but he can
 * not delete nodes under the /foo node.
 */
public class RuleBasedZooKeeperAclProvider implements ACLProvider {

  private final ImmutableList<ACL> defaultAcl;
  private final ImmutableList<Rule> rules;

  public static Builder builder() {
    return new Builder();
  }

  private RuleBasedZooKeeperAclProvider(final ImmutableList<Rule> rules,
                                        final ImmutableList<ACL> defaultAcl) {
    this.rules = rules;
    this.defaultAcl = defaultAcl;
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

  private static class Rule {

    private final Pattern pattern;
    private Id id;
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

  public static class Builder {

    private final List<Rule> rules = Lists.newArrayList();
    private ImmutableList<ACL> defaultAcl = ImmutableList.copyOf(ZooDefs.Ids.READ_ACL_UNSAFE);

    Builder defaultAcl(final ACL... acls) {
      return defaultAcl(Arrays.asList(acls));
    }

    Builder defaultAcl(final List<ACL> defaultAcl) {
      this.defaultAcl = ImmutableList.copyOf(defaultAcl);
      return this;
    }

    Builder rule(final String pathRegex, final int permissions, final Id id) {
      rules.add(new Rule(pathRegex, permissions, id));
      return this;
    }

    RuleBasedZooKeeperAclProvider build() {
      return new RuleBasedZooKeeperAclProvider(ImmutableList.copyOf(rules), defaultAcl);
    }
  }
}
