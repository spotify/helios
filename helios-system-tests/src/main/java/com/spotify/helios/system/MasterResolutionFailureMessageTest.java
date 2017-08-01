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

import static org.junit.Assert.assertTrue;

import com.spotify.helios.cli.CliMain;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.Test;

public class MasterResolutionFailureMessageTest {

  @Test
  public void test() throws Exception {
    final String[] commands = { "jobs", "--no-log-setup", "-d", "bogusdomain" };
    final ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
    new CliMain(new PrintStream(stdout), new PrintStream(stderr), commands).run();
    final String string = stderr.toString();
    assertTrue(string.trim().equals(
        "Failed to resolve helios master in bogusdomain (srv: helios)"));
  }
}
