package com.spotify.helios;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import javax.annotation.Nullable;

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import static org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;

public class ZooKeeperClusterTestManager implements ZooKeeperTestManager {

  protected final Map<Long, QuorumServer> zkPeers = createPeers(3);
  protected final Map<Long, InetSocketAddress> zkAddresses = allocateAddresses(zkPeers);

  protected final Map<Long, QuorumPeer> zkServers = Maps.newHashMap();

  private Path tempDir;
  protected CuratorFramework curator;

  public ZooKeeperClusterTestManager() {
    assert false : "Cannot set up multi-node ZooKeeper clusters with assertions enabled";
    try {
      tempDir = Files.createTempDirectory("helios-zk");
      start();
      final String connect = connectString(zkAddresses);
      final ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
      curator = CuratorFrameworkFactory.newClient(connect, 100, 500, retryPolicy);
      curator.start();
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
  public void start() {
    try {
      for (final Map.Entry<Long, QuorumServer> entry : zkPeers.entrySet()) {
        final Long id = entry.getKey();
        startPeer(id);
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stop() {
    for (final QuorumPeer server : zkServers.values()) {
      server.shutdown();
      try {
        server.join();
      } catch (InterruptedException ignore) {
      }
    }
  }

  public void stopPeer(final long id) throws Exception {
    final QuorumPeer quorumPeer = zkServers.get(id);
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
    if (zkServers.get(id).isRunning()) {
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

  private Path peerDir(final long id) {return tempDir.resolve(Long.toString(id));}

  private Map<Long, QuorumServer> createPeers(final int n) {
    final ImmutableMap.Builder<Long, QuorumServer> peers = ImmutableMap.builder();
    for (long i = 0; i < n; i++) {
      final int clientPort = PortAllocator.allocatePort("zk-peer-client" + i);
      final int electPort = PortAllocator.allocatePort("zk-peer-elect" + i);
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
        final int port = PortAllocator.allocatePort("zk-client-" + key);
        return new InetSocketAddress("127.0.0.1", port);
      }
    }));
  }
}
