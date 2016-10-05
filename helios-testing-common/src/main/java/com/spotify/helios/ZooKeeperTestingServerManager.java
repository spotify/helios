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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.io.FileUtils.deleteQuietly;

import com.google.common.base.Throwables;
import com.google.common.io.Files;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A ZooKeeperTestManager that uses the {@link org.apache.curator.test.TestingServer}
 * to run an in-process ZooKeeper instance.
 */
public class ZooKeeperTestingServerManager implements ZooKeeperTestManager {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperTestingServerManager.class);

  private static final String SUPER_USER = "super";
  private static final String SUPER_PASSWORD = "hunter2****";

  public final TemporaryPorts temporaryPorts = TemporaryPorts.create();

  private final int port = temporaryPorts.localPort("zookeeper");
  private final String endpoint = "127.0.0.1:" + port;
  private final File dataDir;

  private CuratorFramework curator;
  private TestingServer server;

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

  public ZooKeeperTestingServerManager() {
    this.dataDir = Files.createTempDir();

    final ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);

    final Builder builder = CuratorFrameworkFactory.builder()
        .connectString(endpoint)
        .retryPolicy(retryPolicy)
        .authorization("digest", (SUPER_USER + ":" + SUPER_PASSWORD).getBytes());

    curator = builder.build();

    log.info("starting CuratorFramework connected to {}", endpoint);

    curator.start();
    start();
  }

  @Override
  public void ensure(String path) throws Exception {
    curator.newNamespaceAwareEnsurePath(path).ensure(curator.getZookeeperClient());
  }

  @Override
  public void close() throws InterruptedException {
    try {
      server.close();
    } catch (IOException e) {
      Throwables.propagate(e);
    }

    stop();
    deleteQuietly(dataDir);
  }

  @Override
  public String connectString() {
    return endpoint;
  }

  @Override
  public CuratorFramework curatorWithSuperAuth() {
    return curator;
  }

  @Override
  public void awaitUp(long timeout, TimeUnit timeunit) throws TimeoutException {
    Polling.awaitUnchecked(timeout, timeunit, (Callable<Object>) () -> {
      try {
        return curatorWithSuperAuth().usingNamespace(null).getChildren().forPath("/");
      } catch (Exception e) {
        return null;
      }
    });
  }

  @Override
  public void awaitDown(int timeout, TimeUnit timeunit) throws TimeoutException {
    Polling.awaitUnchecked(timeout, timeunit, (Callable<Object>) () -> {
      try {
        curatorWithSuperAuth().usingNamespace(null).getChildren().forPath("/");
        return null;
      } catch (KeeperException.ConnectionLossException e) {
        return true;
      } catch (Exception e) {
        return null;
      }
    });
  }

  @Override
  public void start() {
    log.info("starting zookeeper TestingServer at port={}, dataDir={}", port, dataDir);
    try {
      server = new TestingServer(port, dataDir);
      awaitUp(2, MINUTES);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stop() throws InterruptedException {
    try {
      server.stop();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
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

  public void reset() {
    FileUtils.deleteQuietly(dataDir);
  }
}
