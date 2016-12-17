/*-
 * -\-\-
 * Helios System Tests
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

package com.spotify.helios.system;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

import com.spotify.helios.Polling;
import com.spotify.helios.common.Json;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;

public class CliMasterListTest extends SystemTestBase {

  @Before
  public void initialize() throws Exception {
    startDefaultMaster();

    // Wait for master to come up
    Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<String>() {
      @Override
      public String call() throws Exception {
        final String output = cli("masters");
        return output.contains(masterName()) ? output : null;
      }
    });
  }

  @Test
  public void testMasterListJson() throws Exception {
    final String jsonOutput = cli("masters", "-f", "--json");
    final List<?> masterList = Json.read(jsonOutput, List.class);
    final List<String> expectedList = Lists.newArrayList(TEST_MASTER);
    assertEquals(expectedList, masterList);
  }
}
