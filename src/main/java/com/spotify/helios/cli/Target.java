/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.lang.String.format;

/**
 * A target cluster identified by an endpoint string that can be used with a {@link
 * com.spotify.helios.common.Client}.
 */
public class Target {

  private final String name;
  private final String endpoint;

  Target(final String name, final String endpoint) {
    this.name = name;
    this.endpoint = endpoint;
  }

  public String getName() {
    return name;
  }

  public String getEndpoint() {
    return endpoint;
  }

  /**
   * Create targets from a list of explicit endpoints
   */
  public static List<Target> targetsFrom(final Iterable<String> endpoints) {
    final ImmutableList.Builder<Target> builder = ImmutableList.builder();
    for (final String endpoint : endpoints) {
      builder.add(new Target(endpoint, endpoint));
    }
    return builder.build();
  }

  /**
   * Create targets for a list of sites
   */
  public static List<Target> targetsFrom(final String srvName, final Iterable<String> sites) {
    final ImmutableList.Builder<Target> builder = ImmutableList.builder();
    for (final String site : sites) {
      builder.add(targetFrom(srvName, site));
    }
    return builder.build();
  }

  /**
   * Create a target for a site
   */
  public static Target targetFrom(final String srvName, final String site) {
    return new Target(site, endpoint(srvName, site));
  }

  /**
   * Transform a site into a fully qualified endpoint. E.g. lon -> srv://helios-master.lon.spotify.net.
   */
  private static String endpoint(final String name, final String site) {
    final String domain;
    if (site.contains("spotify.net") || site.endsWith(".")) {
      domain = site;
    } else {
      domain = site + ".spotify.net.";
    }
    return format("srv://%s.%s", name, domain);
  }

}
