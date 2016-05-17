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

package com.spotify.helios.testing;

import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.system.SystemTestBase;

import org.junit.Before;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

abstract class TemporaryJobsTestBase extends SystemTestBase {

  // These static fields exist as a way for nested tests to access non-static fields and methods in
  // SystemTestBase. This is a bit ugly, but we can't pass the values to FakeTest, because we don't
  // instantiate it, JUnit does in the PrintableResult.testResult method. And since JUnit
  // instantiates it, it must be a static class, which means it can't access the non-static fields
  // in SystemTestBase.
  protected static String testTag;

  protected static Path prefixDirectory;

  protected static final class TestProber extends DefaultProber {

    @Override
    public boolean probe(final String host, final PortMapping portMapping) {
      // Probe for ports where docker is running instead of on the mock testHost address
      return super.probe(DOCKER_HOST.address(), portMapping);
    }
  }

  @Before
  public void temporaryJobsSetup() throws Exception {
    testTag = super.testTag;
    prefixDirectory = temporaryFolder.newFolder().toPath();
  }

  public static Map<String, String> emptyEnv() {
    return Collections.emptyMap();
  }

  public static TemporaryJobs.Builder temporaryJobsBuilder() {
    return TemporaryJobs.builder(emptyEnv());
  }
}
