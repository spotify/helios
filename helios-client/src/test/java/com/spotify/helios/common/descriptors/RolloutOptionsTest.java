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

package com.spotify.helios.common.descriptors;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RolloutOptionsTest {

  @Test
  public void verifyBuilder() {
    final RolloutOptions.Builder builder = RolloutOptions.newBuilder();

    // Input to setXXX
    final long setTimeout = 1000;
    final int setParallelism = 2;
    final boolean setMigrate = true;
    final boolean setOverlap = true;
    final String setToken = "derp";

    // Check setXXX methods
    builder.setTimeout(setTimeout);
    builder.setParallelism(setParallelism);
    builder.setMigrate(setMigrate);
    builder.setOverlap(setOverlap);
    builder.setToken(setToken);

    assertEquals("timeout", setTimeout, builder.getTimeout());
    assertEquals("parallellism", setParallelism, builder.getParallelism());
    assertEquals("migrate", setMigrate, builder.getMigrate());
    assertEquals("overlap", setOverlap, builder.getOverlap());
    assertEquals("token", setToken, builder.getToken());

    // Check final output
    final RolloutOptions options = builder.build();
    assertEquals("timeout", setTimeout, options.getTimeout());
    assertEquals("parallellism", setParallelism, options.getParallelism());
    assertEquals("migrate", setMigrate, options.getMigrate());
    assertEquals("overlap", setOverlap, options.getOverlap());
    assertEquals("token", setToken, options.getToken());
  }
}
