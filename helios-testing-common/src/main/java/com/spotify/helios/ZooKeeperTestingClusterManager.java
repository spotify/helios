/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Rule;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import static com.google.common.collect.Iterables.transform;
import static java.util.Arrays.asList;
import static org.apache.commons.io.FileUtils.deleteDirectory;

/**
 * A ZooKeeperTestManager that uses the {@link org.apache.curator.test.TestingServer}
 * to run an in-process ZooKeeper cluster.
 */
public class ZooKeeperTestingClusterManager implements ZooKeeperTestManager {

  @Rule public final TemporaryPorts temporaryPorts = TemporaryPorts.create();

  private static final String SUPER_USER = "super";
  private static final String SUPER_PASSWORD = "hunter2****";

  private final Path tempDir;

  private List<InstanceSpec> zkPeers;
  private List<InetSocketAddress> zkAddresses;
  private List<TestingZooKeeperServer> zkServers;
  private List<CuratorFramework> peerCurators;

  private CuratorFramework curator;
  private TestingCluster cluster;

  static {
    try {
      final String superDigest = DigestAuthenticationProvider.generateDigest(
          SUPER_USER + ":" + SUPER_PASSWORD);

      final Field digestField = DigestAuthenticationProvider.class.getDeclaredField("superDigest");
      digestField.setAccessible(true);

      final Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(digestField, digestField.getModifiers() & ~Modifier.FINAL);

      digestField.set(null, superDigest);
    } catch (NoSuchAlgorithmException | IllegalAccessException | NoSuchFieldException e) {
      // i mean, for real?
      throw Throwables.propagate(e);
    }
  }

  public ZooKeeperTestingClusterManager() {
    try {
      tempDir = Files.createTempDirectory("helios-zk");
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    start();
  }

  @Override
  public void ensure(String path) throws Exception {
    curator.newNamespaceAwareEnsurePath(path).ensure(curator.getZookeeperClient());
  }

  @Override
  public void close() {
    try {
      for (CuratorFramework curator : peerCurators) {
        curator.close();
      }
      curator.close();
      stop();
      deleteDirectory(tempDir.toFile());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String connectString() {
    return connectString(zkAddresses);
  }

  @Override
  public CuratorFramework curatorWithSuperAuth() {
    return curator;
  }

  @Override
  public void awaitUp(long timeout, TimeUnit timeunit) throws TimeoutException {
    Polling.awaitUnchecked(timeout, timeunit, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        try {
          return curatorWithSuperAuth().getChildren().forPath("/");
        } catch (Exception e) {
          return null;
        }
      }
    });
  }

  @Override
  public void awaitDown(int timeout, TimeUnit timeunit) throws TimeoutException {
    Polling.awaitUnchecked(timeout, timeunit, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        try {
          curatorWithSuperAuth().getChildren().forPath("/");
          return null;
        } catch (KeeperException.ConnectionLossException e) {
          return true;
        } catch (Exception e) {
          return null;
        }
      }
    });
  }

  @Override
  public void start() {
    try {
      start0();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    curator = createCurator(connectString(zkAddresses));

    try {
      awaitUp(2, TimeUnit.MINUTES);
    } catch (TimeoutException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stop() {
    try {
      cluster.stop();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void start0() {
    zkPeers = createPeers(3);
    zkAddresses = allocateAddresses(zkPeers);
    peerCurators = createCurators(zkAddresses);

    System.setProperty("zookeeper.jmx.log4j.disable", "true");
    cluster = new TestingCluster(zkPeers);

    zkServers = cluster.getServers();

    try {
      cluster.start();
    } catch (Exception e) {
      stop();
      throw Throwables.propagate(e);
    }
  }

  private List<CuratorFramework> createCurators(final List<InetSocketAddress> addresses) {
    final ImmutableList.Builder<CuratorFramework> curators = ImmutableList.builder();
    for (InetSocketAddress address : addresses) {
      curators.add(createCurator(connectString(address)));
    }

    return curators.build();
  }

  private CuratorFramework createCurator(final String connectString) {
    final ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework curator = CuratorFrameworkFactory.builder()
        .connectString(connectString)
        .retryPolicy(retryPolicy)
        .authorization("digest", (SUPER_USER + ":" + SUPER_PASSWORD).getBytes())
        .build();
    curator.start();
    return curator;
  }

  public void startPeer(final int id) {
    if (zkServers.get(id).getQuorumPeer().isRunning()) {
      throw new IllegalStateException("peer is already running: " + id);
    }

    try {
      zkServers.get(id).restart();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void stopPeer(final int id) {
    try {
      zkServers.get(id).stop();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void resetPeer(final int id) {
    if (zkServers.get(id).getQuorumPeer().isRunning()) {
      throw new IllegalStateException("peer is still running: " + id);
    }

    final Path peerDir = peerDir(id);
    try {
      // Wipe and recreate the data directory
      FileUtils.deleteDirectory(peerDir.toFile());
      Files.createDirectory(peerDir);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Get a {@link CuratorFramework} client connected to only one of the peers.
   *
   * @param id The curator ID.
   * @return The curator.
   */
  public CuratorFramework peerCurator(final int id) {
    return peerCurators.get(id);
  }

  private String connectString(final InetSocketAddress... addresses) {
    return connectString(asList(addresses));
  }

  private String connectString(final Iterable<InetSocketAddress> addresses) {
    return Joiner.on(',').join(endpoints(addresses));
  }

  private Path peerDir(final int id) {
    return tempDir.resolve(Long.toString(id));
  }

  private List<InstanceSpec> createPeers(final int n) {
    final ImmutableList.Builder<InstanceSpec> peers = ImmutableList.builder();

    for (int i = 0; i < n; i++) {
      final int port = temporaryPorts.localPort("zk-client" + i);
      final int electionPort = temporaryPorts.localPort("zk-elect" + i);
      final int quorumPort = temporaryPorts.localPort("zk-quorum" + i);

      final Path peerDir = peerDir(i);
      try {
        Files.createDirectory(peerDir);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }

      final InstanceSpec spec = new InstanceSpec(
          peerDir.toFile(),
          port,
          electionPort,
          quorumPort,
          true,
          i);

      peers.add(spec);
    }

    return peers.build();
  }

  private List<String> endpoints(final Iterable<InetSocketAddress> addresses) {
    return ImmutableList.copyOf(transform(addresses, new Function<InetSocketAddress, String>() {
      @Override
      public String apply(final InetSocketAddress addr) {
        return addr.getHostString() + ":" + addr.getPort();
      }
    }));
  }

  private List<InetSocketAddress> allocateAddresses(final List<InstanceSpec> peers) {
    return ImmutableList.copyOf(transform(
        peers, new Function<InstanceSpec, InetSocketAddress>() {
          @Override
          public InetSocketAddress apply(@Nullable final InstanceSpec spec) {
            return new InetSocketAddress("127.0.0.1", spec.getPort());
          }
        }));
  }
}
