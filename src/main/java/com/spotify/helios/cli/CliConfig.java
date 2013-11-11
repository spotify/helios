/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli;

import com.google.common.base.Joiner;

import com.spotify.config.SpotifyConfigNode;
import com.spotify.helios.common.Defaults;
import com.spotify.json.SpJSON;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class CliConfig {

  private static final String CONFIG_PATH = ".helios" + File.separator + "config";

  private final String username;
  private final List<String> sites;
  private final String srvName;

  public CliConfig(List<String> sites, String srvName) {
    this.username = System.getProperty("user.name");
    this.sites = sites;
    this.srvName = srvName;
  }

  public String getUsername() {
    return username;
  }

  public List<String> getSites() {
    return sites;
  }

  public String getSitesString() {
    return Joiner.on(",").join(sites);
  }

  public String getSrvName() {
    return srvName;
  }

  /**
   * Returns a CliConfig instance with values from a config file from under the users home
   * directory:
   *
   * <user.home>/.helios/config
   *
   * If the file is not found, a CliConfig with pre-defined values will be returned.
   *
   * @throws IOException   If the file exists but could not be read
   * @throws JSONException If the file exists but could not be parsed
   */
  public static CliConfig fromUserConfig()
      throws IOException, JSONException {
    final String userHome = System.getProperty("user.home");
    final String defaults = userHome + File.separator + CONFIG_PATH;
    final File defaultsFile = new File(defaults);

    return fromFile(defaultsFile);
  }

  /**
   * Returns a CliConfig instance with values parsed from the specified file.
   *
   * If the file is not found, a CliConfig with pre-defined values will be returned.
   *
   * @param defaultsFile The file to parse from
   * @throws IOException   If the file exists but could not be read
   * @throws JSONException If the file exists but could not be parsed
   */
  public static CliConfig fromFile(File defaultsFile) throws IOException, JSONException {
    final SpotifyConfigNode config;
    if (defaultsFile.exists() && defaultsFile.canRead()) {
      final JSONObject defaultsJson = SpJSON.newJSONObject(defaultsFile);
      config = new SpotifyConfigNode(defaultsJson);
    } else {
      config = new SpotifyConfigNode(SpJSON.newJSONObject("{}"));
    }

    return fromConfigNode(config);
  }

  /**
   * Returns a CliConfig instance with values parsed from the specified config node.
   *
   * Any value missing in the config tree will get a pre-defined default value.
   */
  public static CliConfig fromConfigNode(SpotifyConfigNode config) {
    checkNotNull(config);

    final List<String> sites = config.getList("sites", Defaults.SITES, String.class);
    final String srvName = config.getString("srvName", Defaults.SRV_NAME);

    return new CliConfig(sites, srvName);
  }
}
