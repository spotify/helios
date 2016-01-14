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

package com.spotify.helios.servicescommon.coordination;

import com.spotify.helios.Parallelized;
import com.spotify.helios.ZooKeeperTestingServerManager;

import org.apache.commons.io.FileUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.Callable;

import static com.spotify.helios.Polling.await;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperModelReporter.noop;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.zookeeper.KeeperException.NodeExistsException;
import static org.junit.Assert.assertArrayEquals;

@RunWith(Parallelized.class)
public class ZooKeeperUpdatingPersistentDirectoryTest {

  private static final String PARENT_PATH = "/foobar";

  private Path stateFile;
  private Path backupDir;

  private static final byte[] BAR1_DATA = "bar1".getBytes();
  private static final byte[] BAR2_DATA = "bar2".getBytes();
  private static final byte[] BAR3_DATA = "bar2".getBytes();
  private static final String FOO_NODE = "foo";
  private static final String BAZ_NODE = "baz";
  private static final String FOO_PATH = ZKPaths.makePath(PARENT_PATH, FOO_NODE);
  private static final String BAZ_PATH = ZKPaths.makePath(PARENT_PATH, BAZ_NODE);

  private ZooKeeperTestingServerManager zk = new ZooKeeperTestingServerManager();

  private ZooKeeperUpdatingPersistentDirectory sut;

  @Before
  public void setUp() throws Exception {
    backupDir = Files.createTempDirectory("helios-zk-updating-persistent-dir-test-backup-");
    stateFile = Files.createTempFile("helios-zk-updating-persistent-dir-test-", "");
    zk.curatorWithSuperAuth().newNamespaceAwareEnsurePath(PARENT_PATH).ensure(
        zk.curatorWithSuperAuth().getZookeeperClient());
    setupDirectory();
  }

  private void setupDirectory() throws IOException, InterruptedException {
    final DefaultZooKeeperClient client = new DefaultZooKeeperClient(zk.curatorWithSuperAuth());
    final ZooKeeperClientProvider provider = new ZooKeeperClientProvider(client, noop());
    sut = ZooKeeperUpdatingPersistentDirectory.create("test", provider, stateFile, PARENT_PATH);
    sut.startAsync();
  }

  @After
  public void tearDown() throws Exception {
    sut.stopAsync().awaitTerminated();
    zk.close();
    FileUtils.deleteQuietly(stateFile.toFile());
    FileUtils.deleteQuietly(backupDir.toFile());
  }

  @Test
  public void verifyCreatesAndRemovesNode() throws Exception {
    sut.put(FOO_NODE, BAR1_DATA);
    final byte[] remote = awaitNode(FOO_PATH);
    assertArrayEquals(BAR1_DATA, remote);
    sut.remove(FOO_NODE);
    awaitNoNode(FOO_PATH);
  }

  @Test
  public void verifyUpdatesDifferingNode() throws Exception {
    try {
      zk.curatorWithSuperAuth().create().forPath(FOO_PATH, "old".getBytes());
    } catch (NodeExistsException ignore) {
    }
    sut.put(FOO_NODE, BAR1_DATA);
    awaitNodeWithData(FOO_PATH, BAR1_DATA);
  }

  @Test
  public void verifyRemovesUndesiredNode() throws Exception {
    zk.ensure(FOO_PATH);
    zk.stop();
    zk.start();
    awaitNoNode(FOO_PATH);
  }

  @Test
  public void verifyRecoversFromBackupRestoreOnline() throws Exception {
    // Create backup
    try {
      zk.curatorWithSuperAuth().create().forPath("/version", "1".getBytes());
    } catch (NodeExistsException ignore) {
    }
    sut.put(FOO_NODE, BAR1_DATA);
    awaitNodeWithData(FOO_PATH, BAR1_DATA);
    zk.backup(backupDir);

    // Write data after backup
    zk.curatorWithSuperAuth().setData().forPath("/version", "2".getBytes());
    sut.put(FOO_NODE, BAR2_DATA);
    sut.put(BAZ_NODE, BAR3_DATA);
    awaitNodeWithData(FOO_PATH, BAR2_DATA);
    awaitNodeWithData(BAZ_PATH, BAR3_DATA);

    // Restore backup
    zk.stop();
    zk.restore(backupDir);
    zk.start();
    assertArrayEquals("1".getBytes(), zk.curatorWithSuperAuth().getData().forPath("/version"));

    // Verify that latest data is pushed
    awaitNodeWithData(FOO_PATH, BAR2_DATA);
    awaitNodeWithData(BAZ_PATH, BAR3_DATA);
  }

  @Test
  public void verifyRecoversFromBackupRestoreOffline() throws Exception {
    // Create backup
    try {
      zk.curatorWithSuperAuth().create().forPath("/version", "1".getBytes());
    } catch (NodeExistsException ignore) {
    }
    sut.put(FOO_NODE, BAR1_DATA);
    awaitNodeWithData(FOO_PATH, BAR1_DATA);
    zk.backup(backupDir);

    // Write data after backup
    zk.curatorWithSuperAuth().setData().forPath("/version", "2".getBytes());
    sut.put(FOO_NODE, BAR2_DATA);
    sut.put(BAZ_NODE, BAR3_DATA);
    awaitNodeWithData(FOO_PATH, BAR2_DATA);
    awaitNodeWithData(BAZ_PATH, BAR3_DATA);

    // Stop persistent directory
    sut.stopAsync().awaitTerminated();

    // Restore backup
    zk.stop();
    zk.restore(backupDir);
    zk.start();
    assertArrayEquals("1".getBytes(), zk.curatorWithSuperAuth().getData().forPath("/version"));

    // Start new persistent directory
    setupDirectory();

    // Verify that latest data is pushed
    awaitNodeWithData(FOO_PATH, BAR2_DATA);
    awaitNodeWithData(BAZ_PATH, BAR3_DATA);
  }

  private byte[] awaitNode(final String path) throws Exception {
    return await(30, SECONDS, new Callable<byte[]>() {
      @Override
      public byte[] call() throws Exception {
        try {
          return zk.curatorWithSuperAuth().getData().forPath(path);
        } catch (KeeperException.NoNodeException e) {
          return null;
        }
      }
    });
  }

  private void awaitNodeWithData(final String path, final byte[] data) throws Exception {
    await(30, SECONDS, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          final byte[] remote = zk.curatorWithSuperAuth().getData().forPath(path);
          return Arrays.equals(data, remote) ? true : null;
        } catch (KeeperException.NoNodeException e) {
          return null;
        }
      }
    });
  }

  private void awaitNoNode(final String path) throws Exception {
    await(30, SECONDS, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          zk.curatorWithSuperAuth().getData().forPath(path);
          return null;
        } catch (KeeperException.NoNodeException e) {
          return true;
        }
      }
    });
  }
}
