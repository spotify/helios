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
  private static final String SITE_NAME = "RaNd0m";
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSite() throws Exception {
    CliConfig.environment = ImmutableMap.of("HELIOS_MASTER", "site://" + SITE_NAME);
    final CliConfig config = CliConfig.fromUserConfig();
    assertEquals(ImmutableList.of(SITE_NAME), config.getSites());
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
  public void testMixtureOfFileAndEnv() throws Exception {
    final File file = temporaryFolder.newFile();
    try (final FileOutputStream outFile = new FileOutputStream(file)) {
      outFile.write(Charsets.UTF_8.encode(
          "{\"masterEndpoints\":[\"http://localhost:5801\"], \"srvName\":\"foo\"}").array());
      CliConfig.environment = ImmutableMap.of("HELIOS_MASTER", "site://" + SITE_NAME);
      final CliConfig config = CliConfig.fromFile(file);

      assertEquals(ImmutableList.of(SITE_NAME), config.getSites());
      assertTrue(config.getMasterEndpoints().isEmpty());
      assertEquals("foo", config.getSrvName());
    }
  }
}
