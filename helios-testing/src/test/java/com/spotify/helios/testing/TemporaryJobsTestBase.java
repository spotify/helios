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

package com.spotify.helios.testing;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.system.SystemTestBase;

import org.junit.Before;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public abstract class TemporaryJobsTestBase extends SystemTestBase {

  // These static fields exist as a way for nested tests to access non-static fields and methods in
  // SystemTestBase. This is a bit ugly, but we can't pass the values to FakeTest, because we don't
  // instantiate it, JUnit does in the PrintableResult.testResult method. And since JUnit
  // instantiates it, it must be a static class, which means it can't access the non-static fields
  // in SystemTestBase.
  protected static HeliosClient client;
  protected static String testHost1;
  protected static String testHost2;
  protected static String testTag;

  protected static Path prefixDirectory;

  protected static final class TestProber extends DefaultProber {

    @Override
    public boolean probe(final String host, final int port) {
      // Probe for ports where docker is running instead of on the mock testHost address
      assertEquals(testHost1, host);
      return super.probe(DOCKER_HOST.address(), port);
    }
  }

  @Before
  public void temporaryJobsSetup() throws Exception {
    startDefaultMaster();
    client = defaultClient();
    testHost1 = testHost() + "1";
    testHost2 = testHost() + "2";
    testTag = super.testTag;
    startDefaultAgent(testHost1);
    startDefaultAgent(testHost2);

    awaitHostStatus(client, testHost1, UP, LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(client, testHost2, UP, LONG_WAIT_SECONDS, SECONDS);

    prefixDirectory = temporaryFolder.newFolder().toPath();
  }

  public static Map<String, String> emptyEnv() {
    return Collections.emptyMap();
  }

  public static TemporaryJobs.Builder temporaryJobsBuilder() {
    return TemporaryJobs.builder(emptyEnv());
  }

  public static TemporaryJobs.Builder temporaryJobsBuilder(final String profile) {
    return TemporaryJobs.builder(profile, emptyEnv());
  }
}
