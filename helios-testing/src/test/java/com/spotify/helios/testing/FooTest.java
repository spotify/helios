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

import org.junit.Rule;
import org.junit.Test;

public class FooTest {

  @Rule
  public TemporaryJobs temporaryJobs = TemporaryJobs.create();

  @Test
  public void test() throws Exception {
    final TemporaryJob memcacheServerJob = temporaryJobs.job()
        .image("registry.spotify.net/spotify/lasertag-memcached:latest")
        .port("memcached", 11211)
        .deploy();
    System.out.println("hi");
  }

}
