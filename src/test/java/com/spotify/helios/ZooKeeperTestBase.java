package com.spotify.helios;

import com.google.common.io.Files;

import com.spotify.logging.UncaughtExceptionLogger;

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

  @Before
  public void setUp() throws Exception {
    UncaughtExceptionLogger.setDefaultUncaughtExceptionHandler();
    tempDir = Files.createTempDir();

    startZookeeper(tempDir);
  }

  @After
  public void teardown() throws Exception {
    stopZookeeper();

    deleteDirectory(tempDir);
    tempDir = null;
  }

  private void startZookeeper(final File tempDir) throws Exception {
    zkServer = new ZooKeeperServer();
    zkServer.setTxnLogFactory(new FileTxnSnapLog(tempDir, tempDir));
    cnxnFactory = ServerCnxnFactory.createFactory();
    cnxnFactory.configure(new InetSocketAddress(zookeeperPort), 0);
    cnxnFactory.startup(zkServer);
  }

  private void stopZookeeper() throws Exception {
    cnxnFactory.shutdown();
    zkServer.shutdown();
  }

}
