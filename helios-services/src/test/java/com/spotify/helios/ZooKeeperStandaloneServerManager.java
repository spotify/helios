package com.spotify.helios;

import com.google.common.base.Throwables;
import com.google.common.io.Files;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;

import static org.apache.commons.io.FileUtils.deleteQuietly;

public class ZooKeeperStandaloneServerManager implements ZooKeeperTestManager {

  private final int port = PortAllocator.allocatePort("zookeeper");
  private final String endpoint = "127.0.0.1:" + port;
  private final File dataDir;

  private ZooKeeperServer zkServer;
  private ServerCnxnFactory cnxnFactory;

  private CuratorFramework curator;

  public ZooKeeperStandaloneServerManager() {
    this.dataDir = Files.createTempDir();
    start();
    final ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
    curator = CuratorFrameworkFactory.newClient(endpoint, 500, 500, retryPolicy);
    curator.start();
  }

  @Override
  public void ensure(String path) throws Exception {
    curator.newNamespaceAwareEnsurePath(path).ensure(curator.getZookeeperClient());
  }

  @Override
  public void close() {
    curator.close();
    stop();
    deleteQuietly(dataDir);
  }

  @Override
  public String connectString() {
    return endpoint;
  }

  @Override
  public CuratorFramework curator() {
    return curator;
  }

  @Override
  public void start() {
    try {
      zkServer = new ZooKeeperServer();
      zkServer.setTxnLogFactory(new FileTxnSnapLog(dataDir, dataDir));
      zkServer.setTickTime(50);
      zkServer.setMinSessionTimeout(100);
      cnxnFactory = ServerCnxnFactory.createFactory();
      cnxnFactory.configure(new InetSocketAddress(port), 0);
      cnxnFactory.startup(zkServer);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stop() {
    cnxnFactory.shutdown();
    zkServer.shutdown();
  }

  public void backup(final Path destination) {
    try {
      FileUtils.copyDirectory(dataDir, destination.toFile());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public void restore(final Path source) {
    try {
      FileUtils.deleteDirectory(dataDir);
      FileUtils.copyDirectory(source.toFile(), dataDir);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public void reset()  {
    FileUtils.deleteQuietly(dataDir);
  }
}
