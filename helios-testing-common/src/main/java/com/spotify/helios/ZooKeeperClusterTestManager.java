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

package com.spotify.helios;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import static org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;

public class ZooKeeperClusterTestManager implements ZooKeeperTestManager {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperClusterTestManager.class);

  @Rule public final TemporaryPorts temporaryPorts = TemporaryPorts.create();

  protected Map<Long, QuorumServer> zkPeers;
  protected Map<Long, InetSocketAddress> zkAddresses;

  protected final Map<Long, QuorumPeer> zkServers = Maps.newHashMap();

  private Path tempDir;
  protected CuratorFramework curator;

  public ZooKeeperClusterTestManager() {
    assert false : "Cannot set up multi-node ZooKeeper clusters with assertions enabled";
    try {
      tempDir = Files.createTempDirectory("helios-zk");
      while (true) {
        try {
          start0();
          break;
        } catch (BindException ignore) {
          log.warn("zookeeper bind error, retrying");
          Thread.sleep(100);
        }
      }
      final String connect = connectString(zkAddresses);
      final ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
      curator = CuratorFrameworkFactory.newClient(connect, 500, 500, retryPolicy);
      curator.start();
      awaitUp(5, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void ensure(String path) throws Exception {
    curator.newNamespaceAwareEnsurePath(path).ensure(curator.getZookeeperClient());
  }

  @Override
  public void close() {
    try {
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
  public CuratorFramework curator() {
    return curator;
  }

  @Override
  public void awaitUp(long timeout, TimeUnit timeunit) throws TimeoutException {
    Polling.awaitUnchecked(timeout, timeunit, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        try {
          return curator().getChildren().forPath("/");
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
          curator().getChildren().forPath("/");
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
    } catch (BindException e) {
      throw Throwables.propagate(e);
    }
    try {
      awaitUp(5, TimeUnit.MINUTES);
    } catch (TimeoutException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stop() {
    for (long id : ImmutableSet.copyOf(zkServers.keySet())) {
      try {
        stopPeer(id);
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private void start0() throws BindException {
    zkPeers = createPeers(3);
    zkAddresses = allocateAddresses(zkPeers);
    try {
      for (final Map.Entry<Long, QuorumServer> entry : zkPeers.entrySet()) {
        final Long id = entry.getKey();
        startPeer(id);
      }
    } catch (Exception e) {
      stop();
      Throwables.propagateIfInstanceOf(e, BindException.class);
      throw Throwables.propagate(e);
    }
  }

  public void stopPeer(final long id) throws InterruptedException {
    final QuorumPeer quorumPeer = zkServers.remove(id);
    quorumPeer.shutdown();
    quorumPeer.join();
  }

  public void startPeer(final long id) throws Exception {

    final Path dir = peerDir(id);
    Files.createDirectories(dir);

    final int clientPort = zkAddresses.get(id).getPort();
    ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory(clientPort, Integer.MAX_VALUE);

    QuorumPeer quorumPeer = new QuorumPeer();
    quorumPeer.setTxnFactory(new FileTxnSnapLog(dir.toFile(), dir.toFile()));
    quorumPeer.setQuorumPeers(zkPeers);
    quorumPeer.setElectionType(3);
    quorumPeer.setMyid(id);
    quorumPeer.setTickTime(ZooKeeperServer.DEFAULT_TICK_TIME);
    quorumPeer.setInitLimit(5);
    quorumPeer.setSyncLimit(2);
    quorumPeer.setQuorumVerifier(new QuorumMaj(zkPeers.size()));
    quorumPeer.setCnxnFactory(cnxnFactory);
    quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
    quorumPeer.setLearnerType(LearnerType.PARTICIPANT);

    quorumPeer.start();

    zkServers.put(id, quorumPeer);
  }

  public void resetPeer(final long id) {
    final QuorumPeer peer = zkServers.get(id);
    if (peer != null && peer.isRunning()) {
      throw new IllegalStateException();
    }
    try {
      FileUtils.deleteDirectory(peerDir(id).toFile());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private String connectString(final Map<Long, InetSocketAddress> addresses) {
    return Joiner.on(',').join(endpoints(addresses).values());
  }

  private Path peerDir(final long id) {
    return tempDir.resolve(Long.toString(id));
  }

  private Map<Long, QuorumServer> createPeers(final int n) {
    final ImmutableMap.Builder<Long, QuorumServer> peers = ImmutableMap.builder();
    for (long i = 0; i < n; i++) {
      final int clientPort = temporaryPorts.localPort("zk-peer-client" + i);
      final int electPort = temporaryPorts.localPort("zk-peer-elect" + i);
      final InetSocketAddress clientAddr = new InetSocketAddress("127.0.0.1", clientPort);
      final InetSocketAddress electionAddr = new InetSocketAddress("127.0.0.1", electPort);
      peers.put(i, new QuorumServer(i, clientAddr, electionAddr));
    }
    return peers.build();
  }

  private Map<Long, String> endpoints(final Map<Long, InetSocketAddress> peers) {
    return ImmutableMap.copyOf(Maps.transformValues(
        peers, new Function<InetSocketAddress, String>() {
      @Override
      public String apply(final InetSocketAddress addr) {
        return addr.getHostString() + ":" + addr.getPort();
      }
    }));
  }

  private Map<Long, InetSocketAddress> allocateAddresses(final Map<Long, QuorumServer> peers) {
    return ImmutableMap.copyOf(Maps.transformEntries(
        peers, new Maps.EntryTransformer<Long, QuorumServer, InetSocketAddress>() {
      @Override
      public InetSocketAddress transformEntry(@Nullable final Long key,
                                              @Nullable final QuorumServer value) {
        final int port = temporaryPorts.localPort("zk-client-" + key);
        return new InetSocketAddress("127.0.0.1", port);
      }
    }));
  }
}
