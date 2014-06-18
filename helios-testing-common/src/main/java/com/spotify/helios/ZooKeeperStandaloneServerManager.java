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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.io.FileUtils.deleteQuietly;

public class ZooKeeperStandaloneServerManager implements ZooKeeperTestManager {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperStandaloneServerManager.class);

  public final TemporaryPorts temporaryPorts = TemporaryPorts.create();

  private final int port = temporaryPorts.localPort("zookeeper");
  private final String endpoint = "127.0.0.1:" + port;
  private final File dataDir;

  private CuratorFramework curator;
  private Process serverProcess;

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
  public void close() throws InterruptedException {
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
      final String classpath = System.getProperty("java.class.path");
      final String java = System.getProperty("java.home") + "/bin/java";
      final String main = Worker.class.getName();
      final String pid = String.valueOf(pid());
      final ProcessBuilder builder = new ProcessBuilder().command(java, "-cp", classpath,
                                                                  "-Xms64m", "-Xmx64m",
                                                                  "-XX:+TieredCompilation",
                                                                  "-XX:TieredStopAtLevel=1",
                                                                  main,
                                                                  pid,
                                                                  dataDir.toString(),
                                                                  String.valueOf(port));
      serverProcess = builder.start();
      Executors.newSingleThreadExecutor().execute(new Runnable() {
        @Override
        public void run() {
          while (true) {
            try {
              final int exitCode = serverProcess.waitFor();
              if (exitCode != 0) {
                log.warn("zookeeper exited: " + exitCode);
              } else {
                log.info("zookeeper exited: 0");
              }
              return;
            } catch (InterruptedException ignored) {
            }
          }
        }
      });
      awaitUp(5, MINUTES);
    } catch (IOException | TimeoutException e) {
      Throwables.propagate(e);
    }
  }

  private int pid() {
    final String name = ManagementFactory.getRuntimeMXBean().getName();
    final String[] parts = name.split("@");
    return Integer.valueOf(parts[0]);
  }

  @Override
  public void stop() throws InterruptedException {
    serverProcess.destroy();
    serverProcess.waitFor();
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

  public static class Worker {

    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    public static void main(String[] args) throws Exception {
      if (args.length != 3) {
        System.err.println("invalid arguments: " + Arrays.toString(args));
        System.exit(2);
        return;
      }
      final int parent = Integer.valueOf(args[0]);
      final File dataDir = new File(args[1]);
      final int port = Integer.valueOf(args[2]);
      try {
        run(dataDir, port);
        wait(parent);
        System.exit(0);
      } catch (InterruptedException e) {
        System.exit(0);
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(1);
      }
    }

    private static void wait(final int pid) throws InterruptedException, IOException {
      while (true) {
        Thread.sleep(200);
        final String[] cmd = {"ps", "-p", String.valueOf(pid)};
        final int exitCode = Runtime.getRuntime().exec(cmd).waitFor();
        if (exitCode == 1) {
          return;
        }
      }
    }

    private static void run(final File dataDir, final int port)
        throws IOException, InterruptedException {
      final ZooKeeperServer server = new ZooKeeperServer();
      server.setTxnLogFactory(new FileTxnSnapLog(dataDir, dataDir));
      server.setTickTime(50);
      server.setMinSessionTimeout(100);
      final ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
      cnxnFactory.configure(new InetSocketAddress(port), 0);
      cnxnFactory.startup(server);
    }
  }
}
