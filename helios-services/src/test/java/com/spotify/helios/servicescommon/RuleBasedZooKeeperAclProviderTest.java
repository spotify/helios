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

package com.spotify.helios.servicescommon;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.Test;

import static org.apache.zookeeper.ZooDefs.Perms.CREATE;
import static org.apache.zookeeper.ZooDefs.Perms.DELETE;
import static org.apache.zookeeper.ZooDefs.Perms.READ;
import static org.apache.zookeeper.ZooDefs.Perms.WRITE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class RuleBasedZooKeeperAclProviderTest {

  @Test
  public void testSimple() {
    final Id id1 = new Id("some_scheme", "id1");
    final Id id2 = new Id("some_scheme", "id2");
    final RuleBasedZooKeeperAclProvider aclProvider = RuleBasedZooKeeperAclProvider.builder()
        .rule("/foo/baz", DELETE, id1)
        .rule("/foo/bar", CREATE, id1)
        .rule("/foo/qux", READ | WRITE, id2)
        .build();

    assertThat(aclProvider.getAclForPath("/foo/baz"), contains(new ACL(DELETE, id1)));
    assertThat(aclProvider.getAclForPath("/foo/bar"), contains(new ACL(CREATE, id1)));
    assertThat(aclProvider.getAclForPath("/foo/qux"), contains(new ACL(READ | WRITE, id2)));
  }

  @Test
  public void testMultipleMatchingRules() {
    final Id id1 = new Id("some_scheme", "id1");
    final Id id2 = new Id("some_scheme", "id2");
    final RuleBasedZooKeeperAclProvider aclProvider = RuleBasedZooKeeperAclProvider.builder()
        .rule("/foo.*", DELETE, id1)
        .rule("/foo/bar", CREATE, id1)
        .rule(".*", READ, id2)
        .rule("/foo/bar/baz", WRITE, id2)
        .build();

    assertThat(aclProvider.getAclForPath("/foo/bar"), containsInAnyOrder(
        new ACL(CREATE | DELETE, id1), new ACL(READ, id2)));
  }

  @Test
  public void testNoMatchingRules() {
    final Id id = new Id("some_scheme", "id");
    final RuleBasedZooKeeperAclProvider aclProvider = RuleBasedZooKeeperAclProvider.builder()
        .rule("/foo/bar/baz", WRITE, id)
        .build();

    assertNull(aclProvider.getAclForPath("/foo/bar"));
  }

  @Test
  public void testNoRules() {
    final RuleBasedZooKeeperAclProvider aclProvider = RuleBasedZooKeeperAclProvider.builder()
        .build();

    assertNull(aclProvider.getAclForPath("/"));
  }

  @Test
  public void testDefaultAcl() {
    final RuleBasedZooKeeperAclProvider aclProvider = RuleBasedZooKeeperAclProvider.builder()
        .defaultAcl(ZooDefs.Ids.CREATOR_ALL_ACL)
        .build();

    assertEquals(ZooDefs.Ids.CREATOR_ALL_ACL, aclProvider.getDefaultAcl());
  }

  @Test
  public void testDefaultDefaultAcl() {
    final RuleBasedZooKeeperAclProvider aclProvider = RuleBasedZooKeeperAclProvider.builder()
        .build();

    assertEquals(ZooDefs.Ids.READ_ACL_UNSAFE, aclProvider.getDefaultAcl());
  }
}
