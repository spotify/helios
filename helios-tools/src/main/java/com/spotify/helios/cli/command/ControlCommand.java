/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.base.Throwables;

import com.spotify.helios.cli.Target;
import com.spotify.helios.common.HeliosClient;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Strings.repeat;
import static java.lang.String.format;

public abstract class ControlCommand {

  public static final int BATCH_SIZE = 10;
  public static final int QUEUE_SIZE = 1000;

  ControlCommand(final Subparser parser) {

    parser.setDefault("command", this).defaultHelp(true);
  }

  public int run(final Namespace options, final List<Target> targets, final PrintStream out,
                 final PrintStream err, final String username, final boolean json)
      throws IOException, InterruptedException {
    boolean successful = true;

    // Execute the control command over each target cluster
    for (final Target target : targets) {
      if (targets.size() > 1) {
        final List<URI> endpoints = target.getEndpointSupplier().get();
        final String header = format("%s (%s)", target.getName(), endpoints);
        out.println(header);
        out.println(repeat("-", header.length()));
        out.flush();
      }

      successful &= run(options, target, out, err, username, json);

      if (targets.size() > 1) {
        out.println();
      }
    }

    return successful ? 0 : 1;
  }

  /**
   * Execute against a cluster at a specific endpoint
   */
  private boolean run(final Namespace options, final Target target, final PrintStream out,
                      final PrintStream err, final String username, final boolean json)
      throws InterruptedException, IOException {

    final List<URI> endpoints = target.getEndpointSupplier().get();
    if (endpoints.size() == 0) {
      err.println("Failed to resolve helios master in " + target);
      return false;
    }

    final HeliosClient client = HeliosClient.newBuilder()
        .setEndpointSupplier(target.getEndpointSupplier())
        .setUser(username)
        .build();

    try {
      final int result = run(options, client, out, json);
      return result == 0;
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      // if target is a site, print message like
      // "Request timed out to master in ash.spotify.net (http://ash2-helios-a4.ash2.spotify.net)",
      // otherwise "Request timed out to master http://ash2-helios-a4.ash2.spotify.net:5800"
      if (cause instanceof TimeoutException) {
        err.println("Request timed out to master in " + target);
      } else {
        throw Throwables.propagate(cause);
      }
      return false;
    } finally {
      client.close();
    }
  }

  abstract int run(final Namespace options, final HeliosClient client, PrintStream out,
                   final boolean json)
      throws ExecutionException, InterruptedException, IOException;
}
