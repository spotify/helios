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

import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface ZooKeeperTestManager {

  /**
   * Start zookeeper.
   */
  void start();

  /**
   * Stop zookeeper.
   */
  void stop();

  /**
   * Ensure a path.
   */
  void ensure(String path) throws Exception;

  /**
   * Tear down zookeeper.
   */
  void close();

  /**
   * Get a connection string for this zk cluster.
   */
  String connectString();

  /**
   * Get a curator client connected to this cluster.
   */
  CuratorFramework curator();

  /**
   * Await zookeeper successfully serving requests.
   */
  void awaitUp(long timeout, TimeUnit timeunit) throws TimeoutException;

  /**
   * Await zookeeper not able to serve requests.
   */
  void awaitDown(int timeout, TimeUnit timeunit) throws TimeoutException;
}
