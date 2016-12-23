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

import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static com.google.common.collect.Sets.newHashSet;
import static com.spotify.helios.servicescommon.ZooKeeperAclProviders.digest;
import static com.spotify.helios.servicescommon.ZooKeeperAclProviders.heliosAclProvider;

import com.google.common.collect.Lists;
import com.spotify.helios.servicescommon.coordination.CuratorClientFactoryImpl;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;

public class ZooKeeperAclInitializer {

  public static void main(final String[] args) throws Exception {
    if (args.length == 6) {
      initializeAcl(args[0], args[1], args[2], args[3], args[4], args[5]);
    } else {
      System.out.println("usage: <ZK connect string> <ZK cluster ID> "
                         + "<master user> <master password> <agent user> <agent password>");
      System.exit(-1);
    }
  }

  static void initializeAcl(final String zooKeeperConnectionString,
                            final String zooKeeperClusterId,
                            final String masterUser,
                            final String masterPassword,
                            final String agentUser,
                            final String agentPassword)
      throws KeeperException {
    final ACLProvider aclProvider = heliosAclProvider(
        masterUser, digest(masterUser, masterPassword),
        agentUser, digest(agentUser, agentPassword));
    final List<AuthInfo> authorization = Lists.newArrayList(new AuthInfo(
        "digest", String.format("%s:%s", masterUser, masterPassword).getBytes()));

    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework curator = new CuratorClientFactoryImpl().newClient(
        zooKeeperConnectionString,
        (int) TimeUnit.SECONDS.toMillis(60),
        (int) TimeUnit.SECONDS.toMillis(15),
        zooKeeperRetryPolicy,
        aclProvider,
        authorization);

    final ZooKeeperClient client = new DefaultZooKeeperClient(curator, zooKeeperClusterId);
    try {
      client.start();
      initializeAclRecursive(client, "/", aclProvider);
    } finally {
      client.close();
    }
  }

  static void initializeAclRecursive(final ZooKeeperClient client, final String path,
                                     final ACLProvider aclProvider)
      throws KeeperException {
    try {
      final List<ACL> expected = aclProvider.getAclForPath(path);
      final List<ACL> actual = client.getAcl(path);

      if (newHashSet(expected).equals(newHashSet(actual))) {
        // actual ACL matches expected
      } else {
        client.setAcl(path, expected);
      }

      for (final String child : client.getChildren(path)) {
        initializeAclRecursive(client, path.replaceAll("/$", "") + "/" + child, aclProvider);
      }
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }
}
