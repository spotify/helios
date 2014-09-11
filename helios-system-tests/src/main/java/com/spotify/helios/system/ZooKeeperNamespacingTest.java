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

package com.spotify.helios.system;

import com.spotify.helios.common.descriptors.HostStatus.Status;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class ZooKeeperNamespacingTest extends SystemTestBase {

  private static final String PATH_PREFIX = "/testing";

  @BeforeClass
  public static void setUpClass() {
    zkPathPrefix = PATH_PREFIX;
  }

  @Test
  public void test() throws Exception {
    startDefaultMaster("--zk-path-prefix", PATH_PREFIX);
    startDefaultAgent(testHost(), "--zk-path-prefix", PATH_PREFIX);

    awaitHostRegistered(testHost(), LONG_WAIT_MINUTES, MINUTES);
    awaitHostStatus(testHost(), Status.UP, LONG_WAIT_MINUTES, MINUTES);

    // Make a zkclient without going through the normal pipe to validate that we're namespacing
    // properly.
    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework framework = CuratorFrameworkFactory.builder()
        .retryPolicy(zooKeeperRetryPolicy)
        .connectString(zk().connectString())
        .connectionTimeoutMs(30000)
        .sessionTimeoutMs(30000)
        .build();
    framework.start();
    final List<String> result = framework.getChildren().forPath("/");
    Collections.sort(result);
    assertEquals(2, result.size());
    assertEquals("testing", result.get(0));
    assertEquals("zookeeper", result.get(1));
  }
}
