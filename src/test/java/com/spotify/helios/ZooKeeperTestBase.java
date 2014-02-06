package com.spotify.helios;

import com.google.common.io.Files;

import com.spotify.logging.UncaughtExceptionLogger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.net.InetSocketAddress;

import static org.apache.commons.io.FileUtils.deleteDirectory;

public class ZooKeeperTestBase {

  protected final int zookeeperPort = PortAllocator.allocatePort();
  protected final String zookeeperEndpoint = "localhost:" + zookeeperPort;

  private File tempDir;
  private ZooKeeperServer zkServer;
  private ServerCnxnFactory cnxnFactory;

  protected CuratorFramework curator;

  @Before
  public void setUp() throws Exception {
    UncaughtExceptionLogger.setDefaultUncaughtExceptionHandler();
    tempDir = Files.createTempDir();

    startZookeeper();

    curator = CuratorFrameworkFactory.newClient(zookeeperEndpoint,
                                                new ExponentialBackoffRetry(1000, 3));
  }

  public void ensure(String path) throws Exception {
    curator.newNamespaceAwareEnsurePath(path).ensure(curator.getZookeeperClient());
  }

  @After
  public void teardown() throws Exception {
    curator.close();

    stopZookeeper();

    deleteDirectory(tempDir);
    tempDir = null;
  }

  protected void startZookeeper() throws Exception {
    startZookeeper(tempDir);
  }

  private void startZookeeper(final File tempDir) throws Exception {
    zkServer = new ZooKeeperServer();
    zkServer.setTxnLogFactory(new FileTxnSnapLog(tempDir, tempDir));
    cnxnFactory = ServerCnxnFactory.createFactory();
    cnxnFactory.configure(new InetSocketAddress(zookeeperPort), 0);
    cnxnFactory.startup(zkServer);
  }

  protected void stopZookeeper() throws Exception {
    cnxnFactory.shutdown();
    zkServer.shutdown();
  }

}
