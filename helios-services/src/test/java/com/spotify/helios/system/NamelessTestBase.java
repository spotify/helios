/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.spotify.config.SpotifyConfigNode;
import com.spotify.helios.PortAllocator;
import com.spotify.nameless.Service;
import com.spotify.nameless.api.NamelessClient;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class NamelessTestBase extends SystemTestBase {

  private static final Path CONFIG_TEMPLATE =
      Paths.get(NamelessTestBase.class.getResource("/nameless-registry.conf").getFile());

  private com.spotify.nameless.Service nameless;
  private Path configFile;

  protected int namelessPort;
  protected String namelessHost = "127.0.0.1";
  protected String namelessEndpoint;

  protected NamelessClient namelessClient;

  @Before
  public void namelessSetUp() throws Exception {
    final String template = new String(Files.readAllBytes(CONFIG_TEMPLATE), UTF_8);
    final SpotifyConfigNode config = SpotifyConfigNode.fromJSONString(template);
    namelessPort = PortAllocator.allocatePort("nameless");
    namelessEndpoint = "tcp://" + namelessHost + ":" + namelessPort;
    config.set("hermes/address", "tcp://*:" + namelessPort);
    configFile = Files.createTempFile("helios-nameless", ".conf");
    Files.write(configFile, config.toString().getBytes(UTF_8));

    namelessClient = new NamelessClient(hermesClient(namelessEndpoint));

    nameless = new Service();
    nameless.start("--no-log-configuration", "--config=" + configFile);
  }

  @After
  public void namelessTeardown() throws Exception {
    try {
      nameless.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (configFile != null) {
      FileUtils.deleteQuietly(configFile.toFile());
    }
    if (namelessClient != null) {
      namelessClient.close();
    }
  }
}
