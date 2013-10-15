/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.spotify.helios.Entrypoint;
import com.spotify.helios.cli.CliConfig;
import com.spotify.helios.service.Client;
import com.spotify.hermes.service.RequestTimeoutException;
import com.spotify.hermes.service.SendFailureException;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.spotify.helios.cli.command.ControlCommandEntrypoint.Target;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static net.sourceforge.argparse4j.impl.Arguments.append;

public abstract class ControlCommand extends Command {

  private final Argument endpointsArg;
  private final Argument sitesArg;
  private final Argument srvNameArg;
  private final Argument usernameArg;

  ControlCommand(final Subparser parser, final CliConfig cliConfig, final PrintStream out) {
    super(parser, cliConfig, out);

    endpointsArg = parser.addArgument("-z", "--master")
        .action(append())
        .setDefault(Lists.newArrayList())
        .help("master endpoint");

    sitesArg = parser.addArgument("-s", "--sites")
        .help(format("sites (default: %s)", cliConfig.getSitesString()));

    srvNameArg = parser.addArgument("--srv-name")
        .setDefault(cliConfig.getSrvName())
        .help("control srv name");

    usernameArg = parser.addArgument("--username")
        .setDefault(System.getProperty("user.name"))
        .help("username");
  }

  @Override
  public Entrypoint getEntrypoint(Namespace options) {
    // Merge sites and explicit endpoints into cluster control endpoints
    final List<String> explicitEndpoints = options.getList(endpointsArg.getDest());
    final List<Target> explicitTargets = targets(explicitEndpoints);
    final String sitesArgument = options.getString(sitesArg.getDest());
    final String sitesString = sitesArgument == null ? cliConfig.getSitesString() : sitesArgument;
    final Iterable<String> sites;
    if (explicitEndpoints.isEmpty()) {
      sites = filter(asList(sitesString.split(",")), not(equalTo("")));
    } else {
      sites = ImmutableList.of();
    }
    final String srvName = options.getString(srvNameArg.getDest());
    final List<Target> siteTargets = targets(srvName, sites);
    final List<Target> targets = ImmutableList.copyOf(concat(explicitTargets, siteTargets));

    return new ControlCommandEntrypoint(this, options, targets, out);
  }

  /**
   * Execute against a cluster at a specific endpoint
   */
  boolean runControl(final Namespace options, final Target target)
      throws InterruptedException {

    final String username = options.getString(usernameArg.getDest());
    final Client client = Client.newBuilder()
        .setUser(username)
        .setEndpoints(target.endpoint)
        .build();

    try {
      final int result = runControl(options, client);
      return result == 0;
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof SendFailureException) {
        out.println("ERROR: unreachable");
      } else if (cause instanceof RequestTimeoutException) {
        out.println("ERROR: timed out");
      } else {
        throw Throwables.propagate(cause);
      }
      return false;
    } finally {
      client.close();
    }
  }

  /**
   * Create targets from a list of explicit endpoints
   */
  private List<Target> targets(final Iterable<String> endpoints) {
    final ImmutableList.Builder<Target> builder = ImmutableList.builder();
    for (final String endpoint : endpoints) {
      builder.add(new Target(endpoint, endpoint));
    }
    return builder.build();
  }

  /**
   * Create targets for a list of sites
   */
  private List<Target> targets(final String srvName, final Iterable<String> sites) {
    final ImmutableList.Builder<Target> builder = ImmutableList.builder();
    for (final String site : sites) {
      builder.add(target(srvName, site));
    }
    return builder.build();
  }

  /**
   * Create a target for a site
   */
  private Target target(final String srvName, final String site) {
    return new Target(site, endpoint(srvName, site));
  }

  /**
   * Transform a site into a fully qualified endpoint. E.g. lon -> srv://helios-control.lon.spotify.net.
   */
  private String endpoint(final String name, final String site) {
    final String domain;
    if (site.contains("spotify.net") || site.endsWith(".")) {
      domain = site;
    } else {
      domain = site + ".spotify.net.";
    }
    return format("srv://%s.%s", name, domain);
  }

  // control commands are still coupled with the
  // argparse layer. decoupling them gives no
  // immediate benefit.
  abstract int runControl(Namespace options, Client client)
      throws ExecutionException, InterruptedException;
}
