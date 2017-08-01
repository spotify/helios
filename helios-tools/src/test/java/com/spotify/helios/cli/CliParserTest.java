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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CliParserTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String SUBCOMMAND = "jobs";
  private static final String[] ENDPOINTS = {
      "http://master-a1.nyc.com:80",
      "http://master-a2.nyc.com:80",
      "http://master-a3.nyc.com:80",
      };
  private static final String[] DOMAINS = { "foo", "bar", "baz" };
  private static final String SERVICE = "foo-service";
  private static final String SRV = "helios";

  private final ImmutableList<String> singleEndpointArgs = ImmutableList.of(
      SUBCOMMAND, "--master", ENDPOINTS[0], SERVICE
  );

  private static String[] toArray(List<String> args, String... additionalArgs) {
    final List<String> newArgs = Lists.newArrayList(args);
    Collections.addAll(newArgs, additionalArgs);
    return newArgs.toArray(new String[newArgs.size()]);
  }

  @Test
  public void testComputeTargetsSingleEndpoint() throws Exception {
    final CliParser cliParser = new CliParser(toArray(singleEndpointArgs));
    final List<Target> targets = cliParser.getTargets();

    // We expect the specified master endpoint target
    final List<Target> expectedTargets =
        ImmutableList.of(Target.from(URI.create(ENDPOINTS[0])));
    assertEquals(expectedTargets, targets);
  }

  @Test
  public void testComputeTargetsMultipleEndpoints() throws Exception {
    final List<String> argsList = Lists.newArrayList(
        SUBCOMMAND, "-d", Joiner.on(",").join(DOMAINS));
    for (final String endpoint : ENDPOINTS) {
      argsList.add("--master");
      argsList.add(endpoint);
    }
    argsList.add(SERVICE);

    final CliParser cliParser = new CliParser(toArray(argsList));
    final List<Target> targets = cliParser.getTargets();

    // We expect only the specified master endpoint targets since they take precedence over domains
    final List<Target> expectedTargets = Lists.newArrayListWithExpectedSize(ENDPOINTS.length);
    for (final String endpoint : ENDPOINTS) {
      expectedTargets.add(Target.from(URI.create(endpoint)));
    }
    assertEquals(expectedTargets, targets);
  }

  @Test
  public void testComputeTargetsSingleDomain() throws Exception {
    final String[] args = { SUBCOMMAND, "-d", DOMAINS[0], SERVICE };
    final CliParser cliParser = new CliParser(args);
    final List<Target> targets = cliParser.getTargets();

    // We expect the specified domain
    final List<Target> expectedTargets = Target.from(SRV, ImmutableList.of(DOMAINS[0]));
    assertEquals(expectedTargets, targets);
  }

  @Test
  public void testComputeTargetsMultiDomain() throws Exception {
    final String[] args = { SUBCOMMAND, "-d", Joiner.on(",").join(DOMAINS), SERVICE };
    final CliParser cliParser = new CliParser(args);
    final List<Target> targets = cliParser.getTargets();

    // We expect the specified domains
    final List<Target> expectedTargets = Target.from(SRV, Lists.newArrayList(DOMAINS));
    assertEquals(expectedTargets, targets);
  }

  @Test
  public void testComputeTargetsMultipleEndpointsFromConfig() throws Exception {
    final String[] args = { SUBCOMMAND, SERVICE };

    // Create a "~/.helios/config" file, which is the path CliConfig reads by default
    final File configDir = temporaryFolder.newFolder(CliConfig.getConfigDirName());
    final File configFile = new File(configDir.getAbsolutePath() + File.separator
                                     + CliConfig.getConfigFileName());

    // Write configuration to that file
    try (final FileOutputStream outFile = new FileOutputStream(configFile)) {
      final ByteBuffer byteBuffer = Charsets.UTF_8.encode(
          "{\"masterEndpoints\":[\"" + ENDPOINTS[0] + "\", \"" + ENDPOINTS[1] + "\", \""
          + ENDPOINTS[2] + "\"], \"domains\":[\"" + DOMAINS[0] + "\"]}");
      outFile.write(byteBuffer.array(), 0, byteBuffer.remaining());

      // Set user's home directory to this temporary folder
      System.setProperty("user.home", temporaryFolder.getRoot().getAbsolutePath());

      final CliParser cliParser = new CliParser(args);
      final List<Target> targets = cliParser.getTargets();

      // We expect only the specified master endpoint targets since they take precedence over
      // domains
      final List<Target> expectedTargets = ImmutableList.of(
          Target.from(URI.create(ENDPOINTS[0])),
          Target.from(URI.create(ENDPOINTS[1])),
          Target.from(URI.create(ENDPOINTS[2]))
      );
      assertEquals(expectedTargets, targets);
    }
  }

  @Test
  public void testComputeTargetsMultipleDomainsFromConfig() throws Exception {
    final String[] args = { SUBCOMMAND, SERVICE };

    // Create a "~/.helios/config" file, which is the path CliConfig reads by default
    final File configDir = temporaryFolder.newFolder(CliConfig.getConfigDirName());
    final File configFile = new File(configDir.getAbsolutePath() + File.separator
                                     + CliConfig.getConfigFileName());

    // Write configuration to that file
    try (final FileOutputStream outFile = new FileOutputStream(configFile)) {
      final ByteBuffer byteBuffer = Charsets.UTF_8.encode(
          "{\"domains\":[\"" + DOMAINS[0] + "\", \"" + DOMAINS[1] + "\", \"" + DOMAINS[2] + "\"]}");
      outFile.write(byteBuffer.array(), 0, byteBuffer.remaining());

      // Set user's home directory to this temporary folder
      System.setProperty("user.home", temporaryFolder.getRoot().getAbsolutePath());

      final CliParser cliParser = new CliParser(args);
      final List<Target> targets = cliParser.getTargets();

      // We expect the specified domains
      final List<Target> expectedTargets = Target.from(
          SRV, ImmutableList.of(DOMAINS[0], DOMAINS[1], DOMAINS[2]));
      assertEquals(expectedTargets, targets);
    }
  }

  @Test
  public void testInsecureHttpsDisabledByDefault() throws Exception {
    final CliParser parser = new CliParser(toArray(singleEndpointArgs));

    assertFalse("GlobalArg 'insecure' should default to false",
        parser.getNamespace().getBoolean("insecure"));
  }

  @Test
  public void testInsecureHttpsEnable() throws Exception {
    final CliParser parser = new CliParser(toArray(singleEndpointArgs, "--insecure"));

    assertTrue(parser.getNamespace().getBoolean("insecure"));
  }
}
