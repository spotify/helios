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

import com.typesafe.config.Config;

import org.junit.Test;

import java.util.Properties;

import static com.spotify.helios.testing.TemporaryJobs.HELIOS_TESTING_PROFILE;
import static org.junit.Assert.assertEquals;

public class TempJobsProfileOverrideTest {
  @Test
  public void foo() throws Exception {
    final Properties oldProperties = System.getProperties();
    final Config c;
    try {
      System.setProperty(HELIOS_TESTING_PROFILE, "this");
      c = TemporaryJobs.loadConfig();
    } finally {
      System.setProperties(oldProperties);
    }
    assertEquals("this", TemporaryJobs.getProfileFromConfig(c));
  }
}
