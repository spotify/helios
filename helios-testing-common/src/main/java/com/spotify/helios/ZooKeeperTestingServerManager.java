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

import com.google.common.base.Throwables;
import com.google.common.io.Files;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.io.FileUtils.deleteQuietly;

/**
 * A ZooKeeperTestManager that uses the {@link org.apache.curator.test.TestingServer}
 * to run an in-process ZooKeeper instance.
 */
public class ZooKeeperTestingServerManager implements ZooKeeperTestManager {

  public final TemporaryPorts temporaryPorts = TemporaryPorts.create();

  private final int port = temporaryPorts.localPort("zookeeper");
  private final String endpoint = "127.0.0.1:" + port;
  private final File dataDir;

  private CuratorFramework curator;
  private TestingServer server;

  public ZooKeeperTestingServerManager() {
    this(null);
  }

  public ZooKeeperTestingServerManager(final String namespace) {
    this.dataDir = Files.createTempDir();

    final ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);

    Builder builder = CuratorFrameworkFactory.builder()
        .connectString(endpoint)
        .retryPolicy(retryPolicy);

    if (namespace != null) {
      builder = builder.namespace(namespace);
    }

    curator = builder.build();
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
  public CuratorFramework curator() {
    return curator;
  }

  @Override
  public void awaitUp(long timeout, TimeUnit timeunit) throws TimeoutException {
    Polling.awaitUnchecked(timeout, timeunit, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        try {
          return curator().usingNamespace(null).getChildren().forPath("/");
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
          curator().usingNamespace(null).getChildren().forPath("/");
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
