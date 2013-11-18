/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.base.Throwables;

import com.spotify.helios.cli.Target;
import com.spotify.helios.common.Client;
import com.spotify.hermes.service.RequestTimeoutException;
import com.spotify.hermes.service.SendFailureException;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Strings.repeat;
import static java.lang.String.format;

public abstract class ControlCommand {

  private final Logger log = LoggerFactory.getLogger(this.getClass());

  ControlCommand(final Subparser parser) {

    parser.setDefault("command", this).defaultHelp(true);
  }

  public int run(final Namespace options, final List<Target> targets,
                 final PrintStream out, final String username) {
    boolean successful = true;

    // Execute the control command over each target cluster
    for (final Target target : targets) {
      if (targets.size() > 1) {
        final String header = format("%s (%s)", target.getName(), target.getEndpoint());
        out.println(header);
        out.println(repeat("-", header.length()));
        out.flush();
      }

      try {
        successful &= run(options, target, out, username);
      } catch (InterruptedException e) {
        log.error("Error running control command", e);
      }

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
                      final String username)
      throws InterruptedException {

    final Client client = Client.newBuilder()
        .setUser(username)
        .setEndpoints(target.getEndpoint())
        .build();

    try {
      final int result = run(options, client, out);
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

  abstract int run(final Namespace options, final Client client, PrintStream out)
      throws ExecutionException, InterruptedException;
}
