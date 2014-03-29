/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import com.spotify.helios.common.Resolver;

import java.net.URI;
import java.util.List;

/**
 * A target cluster identified by an endpoint string that can be used with a {@link
 * com.spotify.helios.client.HeliosClient}.
 */
public abstract class Target {
  private final String name;

  Target(final String name) {
    this.name = name;
  }

  public abstract Supplier<List<URI>> getEndpointSupplier();

  public String getName() {
    return name;
  }

  private static class SrvTarget extends Target {
    private final String srv;
    private final String domain;

    private SrvTarget(final String srv, final String domain) {
      super(srv);
      this.srv = srv;
      this.domain = domain;
    }

    @Override
    public Supplier<List<URI>> getEndpointSupplier() {
      return Resolver.supplier(srv, domain);
    }

    @Override
    public String toString() {
      return domain + " (srv: " + srv + ")";
    }
  }

  private static class ExplicitTarget extends Target {
    private final List<URI> endpoints;

    private ExplicitTarget(final Iterable<URI> endpoints) {
      super(Joiner.on(',').join(endpoints));
      this.endpoints = ImmutableList.copyOf(endpoints);
    }

    @Override
    public Supplier<List<URI>> getEndpointSupplier() {
      return Suppliers.ofInstance(endpoints);
    }

    @Override
    public String toString() {
      return Joiner.on(',').join(endpoints);
    }
  }

  /**
   * Create a target from a list of explicit endpoints
   */
  public static Target from(final Iterable<URI> endpoints) {
    return new ExplicitTarget(endpoints);
  }

  /**
   * Create targets for a list of sites
   */
  public static List<Target> from(final String srvName, final Iterable<String> sites) {
    final ImmutableList.Builder<Target> builder = ImmutableList.builder();
    for (final String site : sites) {
      builder.add(new SrvTarget(srvName, site));
    }
    return builder.build();
  }
}
