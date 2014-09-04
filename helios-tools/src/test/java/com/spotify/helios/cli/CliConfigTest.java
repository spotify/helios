package com.spotify.helios.cli;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CliConfigTest {

  private static final String ENDPOINT1 = "http://master-a1.nyc.com:80";
  private static final String ENDPOINT2 = "http://master-a2.nyc.com:80";
  private static final String ENDPOINT3 = "http://master-a3.nyc.com:80";
  private static final String SITE1 = "foo";
  private static final String SITE2 = "bar";
  private static final String SITE3 = "baz";

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSite() throws Exception {
    CliConfig.environment = ImmutableMap.of("HELIOS_MASTER", "site://" + SITE1);
    final CliConfig config = CliConfig.fromUserConfig();
    assertEquals(ImmutableList.of(SITE1), config.getSites());
    assertTrue(config.getMasterEndpoints().isEmpty());
  }

  @Test
  public void testHttp() throws Exception {
    final String uri = "http://localhost:5801";
    CliConfig.environment = ImmutableMap.of("HELIOS_MASTER", uri);
    final CliConfig config = CliConfig.fromUserConfig();
    assertEquals(ImmutableList.of(new URI(uri)), config.getMasterEndpoints());
    assertTrue(config.getSites().isEmpty());
  }

  @Test
  public void testConfigFromFile() throws Exception {
    final File file = temporaryFolder.newFile();
    try (final FileOutputStream outFile = new FileOutputStream(file)) {
      outFile.write(Charsets.UTF_8.encode(
          "{\"masterEndpoints\":[\"" + ENDPOINT1 + "\", \"" + ENDPOINT2+ "\", \"" + ENDPOINT3 +
          "\"], \"sites\":[\"" + SITE1 + "\", \"" + SITE2 + "\", \"" + SITE3 +
          "\"], \"srvName\":\"foo\"}").array());
      final CliConfig config = CliConfig.fromFile(file);

      assertEquals(
          ImmutableList.of(URI.create(ENDPOINT1), URI.create(ENDPOINT2), URI.create(ENDPOINT3)),
          config.getMasterEndpoints());
      assertEquals(ImmutableList.of(SITE1, SITE2, SITE3), config.getSites());
      assertEquals("foo", config.getSrvName());
    }
  }

  @Test
  public void testMixtureOfFileAndEnv() throws Exception {
    final File file = temporaryFolder.newFile();
    try (final FileOutputStream outFile = new FileOutputStream(file)) {
      outFile.write(Charsets.UTF_8.encode(
          "{\"masterEndpoints\":[\"http://localhost:5801\"], \"srvName\":\"foo\"}").array());
      CliConfig.environment = ImmutableMap.of("HELIOS_MASTER", "site://" + SITE1);
      final CliConfig config = CliConfig.fromFile(file);

      assertEquals(ImmutableList.of(SITE1), config.getSites());
      assertTrue(config.getMasterEndpoints().isEmpty());
      assertEquals("foo", config.getSrvName());
    }
  }
}
