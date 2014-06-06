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
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.junit.Rule;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.io.FileUtils.deleteQuietly;

public class ZooKeeperStandaloneServerManager implements ZooKeeperTestManager {

  @Rule public final TemporaryPorts temporaryPorts = TemporaryPorts.create();

  private final int port = temporaryPorts.localPort("zookeeper");
  private final String endpoint = "127.0.0.1:" + port;
  private final File dataDir;

  private ZooKeeperServer zkServer;
  private ServerCnxnFactory cnxnFactory;

  private CuratorFramework curator;

  public ZooKeeperStandaloneServerManager() {
    this.dataDir = Files.createTempDir();
    final ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
    curator = CuratorFrameworkFactory.newClient(endpoint, 500, 500, retryPolicy);
    curator.start();
    start();
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
      zkServer = new ZooKeeperServer();
      zkServer.setTxnLogFactory(new FileTxnSnapLog(dataDir, dataDir));
      zkServer.setTickTime(50);
      zkServer.setMinSessionTimeout(100);
      cnxnFactory = ServerCnxnFactory.createFactory();
      cnxnFactory.configure(new InetSocketAddress(port), 0);
      cnxnFactory.startup(zkServer);
      awaitUp(5, MINUTES);
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
