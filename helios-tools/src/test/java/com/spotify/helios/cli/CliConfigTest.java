/*-
 * -\-\-
 * Helios Tools
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

package com.spotify.helios.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigException;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;

public class CliConfigTest {

  private static final String ENDPOINT1 = "http://master-a1.nyc.com:80";
  private static final String ENDPOINT2 = "http://master-a2.nyc.com:80";
  private static final String ENDPOINT3 = "http://master-a3.nyc.com:80";
  private static final String SITE1 = "foo";
  private static final String SITE2 = "bar";
  private static final String SITE3 = "baz";

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule public final ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testSite() throws Exception {
    final Map<String, String> environment = ImmutableMap.of("HELIOS_MASTER", "domain://" + SITE1);
    final CliConfig config = CliConfig.fromUserConfig(environment);
    assertEquals(ImmutableList.of(SITE1), config.getDomains());
    assertTrue(config.getMasterEndpoints().isEmpty());
  }

  @Test
  public void testHttp() throws Exception {
    final String uri = "http://localhost:5801";
    final Map<String, String> environment = ImmutableMap.of("HELIOS_MASTER", uri);
    final CliConfig config = CliConfig.fromUserConfig(environment);
    assertEquals(ImmutableList.of(new URI(uri)), config.getMasterEndpoints());
    assertTrue(config.getDomains().isEmpty());
  }

  @Test
  public void testConfigFromFile() throws Exception {
    final Map<String, String> environment = ImmutableMap.of();
    final File file = temporaryFolder.newFile();
    try (final FileOutputStream outFile = new FileOutputStream(file)) {
      final ByteBuffer byteBuffer = Charsets.UTF_8.encode(
          "{\"masterEndpoints\":[\"" + ENDPOINT1 + "\", \"" + ENDPOINT2 + "\", \"" + ENDPOINT3 +
          "\"], \"domains\":[\"" + SITE1 + "\", \"" + SITE2 + "\", \"" + SITE3 +
          "\"], \"srvName\":\"foo\"}");
      outFile.write(byteBuffer.array(), 0, byteBuffer.remaining());
      final CliConfig config = CliConfig.fromFile(file, environment);

      assertEquals(
          ImmutableList.of(URI.create(ENDPOINT1), URI.create(ENDPOINT2), URI.create(ENDPOINT3)),
          config.getMasterEndpoints());
      assertEquals(ImmutableList.of(SITE1, SITE2, SITE3), config.getDomains());
      assertEquals("foo", config.getSrvName());
    }
  }

  @Test
  public void testConfigFromFileWithInvalidJson() throws Exception {
    final Map<String, String> environment = ImmutableMap.of();
    final File file = temporaryFolder.newFile();
    expectedEx.expect(ConfigException.class);
    expectedEx.expectMessage(Matchers.containsString("Expecting close brace } or a comma"));

    try (final FileOutputStream outFile = new FileOutputStream(file)) {
      outFile.write(Charsets.UTF_8.encode(
          "{\"masterEndpoints\":[\"" + ENDPOINT1 + "\", \"" + ENDPOINT2 + "\", \"" + ENDPOINT3 +
          "\"], \"domains\":[\"" + SITE1 + "\", \"" + SITE2 + "\", \"" + SITE3 +
          "\"], \"srvName\":\"foo\"").array());
      CliConfig.fromFile(file, environment);
    }
  }

  @Test
  public void testMixtureOfFileAndEnv() throws Exception {
    final Map<String, String> environment = ImmutableMap.of("HELIOS_MASTER", "domain://" + SITE1);
    final File file = temporaryFolder.newFile();
    try (final FileOutputStream outFile = new FileOutputStream(file)) {
      final ByteBuffer byteBuffer = Charsets.UTF_8.encode(
          "{\"masterEndpoints\":[\"http://localhost:5801\"], \"srvName\":\"foo\"}");
      outFile.write(byteBuffer.array(), 0, byteBuffer.remaining());

      final CliConfig config = CliConfig.fromFile(file, environment);

      assertEquals(ImmutableList.of(SITE1), config.getDomains());
      assertTrue(config.getMasterEndpoints().isEmpty());
      assertEquals("foo", config.getSrvName());
    }
  }
}
