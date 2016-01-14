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
   * @throws InterruptedException If the thread is interrupted.
   */
  void stop() throws InterruptedException;

  /**
   * Ensure a path.
   * @param path The path to ensure.
   * @throws Exception If an unexpected exception occurs.
   */
  void ensure(String path) throws Exception;

  /**
   * Tear down zookeeper.
   * @throws InterruptedException If the thread is interrupted.
   */
  void close() throws InterruptedException;

  /**
   * Get a connection string for this zk cluster.
   * @return The connection string.
   */
  String connectString();

  /**
   * Get a curator client connected to this cluster.
   * @return The curator.
   */
  CuratorFramework curatorWithSuperAuth();

  /**
   * Await zookeeper successfully serving requests.
   * @param timeout The timeout value.
   * @param timeunit The time unit.
   * @throws TimeoutException If the operation times out.
   */
  void awaitUp(long timeout, TimeUnit timeunit) throws TimeoutException;

  /**
   * Await zookeeper not able to serve requests.
   * @param timeout The timeout value.
   * @param timeunit The timeout unit.
   * @throws TimeoutException If the operation times out.
   */
  void awaitDown(int timeout, TimeUnit timeunit) throws TimeoutException;
}
