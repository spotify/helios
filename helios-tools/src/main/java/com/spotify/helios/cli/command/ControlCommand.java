/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.base.Throwables;

import com.spotify.helios.cli.Target;
import com.spotify.helios.common.Client;
import com.spotify.hermes.Hermes;
import com.spotify.hermes.service.RequestTimeoutException;
import com.spotify.hermes.service.SendFailureException;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Strings.repeat;
import static java.lang.String.format;

public abstract class ControlCommand {

  public static final int BATCH_SIZE = 10;
  public static final int QUEUE_SIZE = 1000;

  private final Logger log = LoggerFactory.getLogger(this.getClass());

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
        final String header = target.getName() == null ? target.getEndpoint() :
                              format("%s (%s)", target.getName(), target.getEndpoint());
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

    final com.spotify.hermes.service.Client hermesClient = Hermes.newClient(target.getEndpoint());

    final com.spotify.hermes.service.Client batchingHermesClient =
        new BatchingHermesClient(hermesClient, BATCH_SIZE, QUEUE_SIZE);

    final Client client = Client.newBuilder()
        .setClient(batchingHermesClient)
        .setUser(username)
        .build();

    try {
      final int result = run(options, client, out, json);
      return result == 0;
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      // if target is a site, print message like
      // "Request timed out to master in ash.spotify.net (srv://helios.services.ash.spotify.net)",
      // otherwise "Request timed out to master tcp://master.ash.spotify.net:5800"
      final String msg = target.getEndpoint().startsWith("srv")
                         ? format("in %s (%s)", target.getName(), target.getEndpoint())
                         : target.getEndpoint();
      if (cause instanceof SendFailureException) {
        err.println("Failure sending message to master " + msg);
      } else if (cause instanceof RequestTimeoutException) {
        err.println("Request timed out to master " + msg);
      } else {
        throw Throwables.propagate(cause);
      }
      return false;
    } finally {
      client.close();
    }
  }

  abstract int run(final Namespace options, final Client client, PrintStream out,
                   final boolean json)
      throws ExecutionException, InterruptedException, IOException;
}
