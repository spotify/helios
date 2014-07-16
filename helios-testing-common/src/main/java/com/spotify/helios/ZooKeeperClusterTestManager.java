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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.common.Json;

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

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import static com.google.common.collect.Iterables.transform;
import static com.spotify.helios.ChildProcesses.Subprocess;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Arrays.asList;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import static org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;

public class ZooKeeperClusterTestManager implements ZooKeeperTestManager {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperClusterTestManager.class);
  public static final TypeReference<Map<Long, ConstructableQuorumServer>> PEERS_TYPE
      = new TypeReference<Map<Long, ConstructableQuorumServer>>() {};

  @Rule public final TemporaryPorts temporaryPorts = TemporaryPorts.create();

  protected Map<Long, QuorumServer> zkPeers;
  protected Map<Long, InetSocketAddress> zkAddresses;
  protected Map<Long, CuratorFramework> peerCurators;

  protected final Map<Long, Subprocess> zkProcesses = Maps.newHashMap();

  private Path tempDir;
  protected CuratorFramework curator;

  public ZooKeeperClusterTestManager() {
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
      curator = createCurator(connectString(zkAddresses.values()));
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
      for (CuratorFramework curator : peerCurators.values()) {
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
    return connectString(zkAddresses.values());
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
    for (long id : ImmutableSet.copyOf(zkProcesses.keySet())) {
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
    peerCurators = createCurators(zkAddresses);
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

  private Map<Long, CuratorFramework> createCurators(final Map<Long, InetSocketAddress> addresses) {
    final ImmutableMap.Builder<Long, CuratorFramework> curators = ImmutableMap.builder();
    for (Map.Entry<Long, InetSocketAddress> entry : addresses.entrySet()) {
      curators.put(entry.getKey(), createCurator(connectString(entry.getValue())));
    }
    return curators.build();
  }

  private CuratorFramework createCurator(final String connectString) {
    final ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework curator = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
    curator.start();
    return curator;
  }

  public void stopPeer(final long id) throws InterruptedException {
    final Subprocess peer = zkProcesses.remove(id);
    peer.kill();
    peer.join();
  }

  public void startPeer(final long id) throws Exception {

    final Path dir = peerDir(id);
    Files.createDirectories(dir);

    final int port = zkAddresses.get(id).getPort();

    final Subprocess peerProcess;
    try {
      peerProcess = ChildProcesses.process()
          .exitParentOnChildExit()
          .main(PeerProcess.class)
          .args(String.valueOf(id), dir.toString(),
                String.valueOf(port), Json.asStringUnchecked(zkPeers))
          .spawn();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    zkProcesses.put(id, peerProcess);
  }

  public void resetPeer(final long id) {
    final Subprocess peer = zkProcesses.get(id);
    if (peer != null) {
      if (peer.running()) {
        throw new IllegalStateException("peer is still running: " + id);
      }
    }
    try {
      FileUtils.deleteDirectory(peerDir(id).toFile());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Get a {@link CuratorFramework} client connected to only one of the peers.
   */
  public CuratorFramework peerCurator(final long id) {
    return peerCurators.get(id);
  }

  private String connectString(final InetSocketAddress... addresses) {
    return connectString(asList(addresses));
  }

  private String connectString(final Iterable<InetSocketAddress> addresses) {
    return Joiner.on(',').join(endpoints(addresses));
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

  private List<String> endpoints(final Iterable<InetSocketAddress> addresses) {
    return ImmutableList.copyOf(transform(addresses, new Function<InetSocketAddress, String>() {
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

  public static class PeerProcess extends ChildProcesses.Child {

    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    public static void main(String[] args) throws Exception {
      if (args.length != 5) {
        System.err.println("invalid arguments: " + Arrays.toString(args));
        System.exit(2);
        return;
      }
      new PeerProcess().run(args);
    }

    @Override
    protected void start(final String[] args) throws Exception {
      final long id = Long.valueOf(args[1]);
      final File dir = new File(args[2]);
      final int port = Integer.valueOf(args[3]);
      final Map<Long, QuorumServer> peers = Json.read(args[4], PEERS_TYPE);
      start(id, dir, port, peers);
    }

    private void start(final long id, final File dir, final int port,
                       final Map<Long, QuorumServer> peers) throws IOException {
      final ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory(port, MAX_VALUE);
      QuorumPeer quorumPeer = new QuorumPeer();
      quorumPeer.setTxnFactory(new FileTxnSnapLog(dir, dir));
      quorumPeer.setQuorumPeers(peers);
      quorumPeer.setElectionType(3);
      quorumPeer.setMyid(id);
      quorumPeer.setTickTime(ZooKeeperServer.DEFAULT_TICK_TIME);
      quorumPeer.setInitLimit(5);
      quorumPeer.setSyncLimit(2);
      quorumPeer.setQuorumVerifier(new QuorumMaj(peers.size()));
      quorumPeer.setCnxnFactory(cnxnFactory);
      quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
      quorumPeer.setLearnerType(LearnerType.PARTICIPANT);
      quorumPeer.start();
    }
  }

  public static class ConstructableQuorumServer extends QuorumServer {

    private ConstructableQuorumServer() {
      super(0, null);
    }
  }
}
