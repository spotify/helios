/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.servicescommon;

import com.spotify.helios.master.HostNotFoundException;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import org.apache.zookeeper.KeeperException;

public interface ZooKeeperRegistrar {

  /**
   * Called upon startup
   *
   * @throws Exception If an unexpected error occurs.
   */
  void startUp() throws Exception;

  /**
   * Called upon shutdown
   *
   * @throws Exception If an unexpected error occurs.
   */
  void shutDown() throws Exception;

  /**
   * Called when ZK client connects. Handler should attempt to do on connection initialization here.
   *
   * @param client The zookeeper client.
   *
   * @return if registration was successful
   *
   * @throws KeeperException       If an unexpected zookeeper error occurs.
   * @throws HostNotFoundException If the hostname we are trying to re-register as doesn't exist.
   */
  boolean tryToRegister(final ZooKeeperClient client)
      throws KeeperException, HostNotFoundException;
}
