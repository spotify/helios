package com.spotify.helios.cli;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CliParserTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String SUBCOMMAND = "jobs";
  private static final String[] ENDPOINTS = {
      "http://master-a1.nyc.com:80",
      "http://master-a2.nyc.com:80",
      "http://master-a3.nyc.com:80",
  };
  private static final String[] DOMAINS = {"foo", "bar", "baz"};
  private static final String SERVICE = "foo-service";
  private static final String SRV = "helios";

  @Before
  public void init() {
    // TODO (dxia) For some reason CliConfig.java:59 never gets called here, but it's called when
    // running CliConfigTest. If I don't clear the environment attribute, there's a stray key -> val
    // of "HELIOS_MASTER" -> "domain://foo" left behind from somewhere that screws up the tests.
    CliConfig.environment = ImmutableMap.of();
  }

  @Test
  public void testComputeTargetsSingleEndpoint() throws Exception {
    final String[] args = {SUBCOMMAND, "--master", ENDPOINTS[0], SERVICE};
    final CliParser cliParser = new CliParser(args);
    final List<Target> targets = cliParser.getTargets();

    // We expect the specified master endpoint target
    final List<Target> expectedTargets =
        ImmutableList.of(Target.from(URI.create(ENDPOINTS[0])));
    assertEquals(expectedTargets, targets);
  }

  @Test
  public void testComputeTargetsMultipleEndpoints() throws Exception {
    final List<String> argsList = Lists.newArrayList(SUBCOMMAND, "-d", Joiner.on(",").join(DOMAINS));
    for (String endpoint : ENDPOINTS) {
      argsList.add("--master");
      argsList.add(endpoint);
    }
    argsList.add(SERVICE);

    final String[] args = new String[argsList.size()];
    argsList.toArray(args);

    final CliParser cliParser = new CliParser(args);
    final List<Target> targets = cliParser.getTargets();

    // We expect only the specified master endpoint targets since they take precedence over domains
    final List<Target> expectedTargets = Lists.newArrayListWithExpectedSize(ENDPOINTS.length);
    for (String endpoint : ENDPOINTS) {
      expectedTargets.add(Target.from(URI.create(endpoint)));
    }
    assertEquals(expectedTargets, targets);
  }

  @Test
  public void testComputeTargetsSingleDomain() throws Exception {
    final String[] args = {SUBCOMMAND, "-d", DOMAINS[0], SERVICE};
    final CliParser cliParser = new CliParser(args);
    final List<Target> targets = cliParser.getTargets();

    // We expect the specified domain
    final List<Target> expectedTargets = Target.from(SRV, ImmutableList.of(DOMAINS[0]));
    assertEquals(expectedTargets, targets);
  }

  @Test
  public void testComputeTargetsMultiDomain() throws Exception {
    final String[] args = {SUBCOMMAND, "-d", Joiner.on(",").join(DOMAINS), SERVICE};
    final CliParser cliParser = new CliParser(args);
    final List<Target> targets = cliParser.getTargets();

    // We expect the specified domains
    final List<Target> expectedTargets = Target.from(SRV, Lists.newArrayList(DOMAINS));
    assertEquals(expectedTargets, targets);
  }

  @Test
  public void testComputeTargetsMultipleEndpointsFromConfig() throws Exception {
    final String[] args = {SUBCOMMAND, SERVICE};

    // Create a "~/.helios/config" file, which is the path CliConfig reads by default
    final File configDir = temporaryFolder.newFolder(CliConfig.getConfigDirName());
    final File configFile = new File(configDir.getAbsolutePath() + File.separator +
                                     CliConfig.getConfigFileName());

    // Write configuration to that file
    try (final FileOutputStream outFile = new FileOutputStream(configFile)) {
      outFile.write(Charsets.UTF_8.encode(
          "{\"masterEndpoints\":[\"" + ENDPOINTS[0] + "\", \"" +ENDPOINTS[1] + "\", \"" +
          ENDPOINTS[2] + "\"], \"domains\":[\"" + DOMAINS[0] + "\"]}").array());

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
    final String[] args = {SUBCOMMAND, SERVICE};

    // Create a "~/.helios/config" file, which is the path CliConfig reads by default
    final File configDir = temporaryFolder.newFolder(CliConfig.getConfigDirName());
    final File configFile = new File(configDir.getAbsolutePath() + File.separator +
                                     CliConfig.getConfigFileName());

    // Write configuration to that file
    try (final FileOutputStream outFile = new FileOutputStream(configFile)) {
      outFile.write(Charsets.UTF_8.encode(
          "{\"domains\":[\"" + DOMAINS[0] + "\", \"" + DOMAINS[1] + "\", \"" + DOMAINS[2] + "\"]}")
                        .array());

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
}
