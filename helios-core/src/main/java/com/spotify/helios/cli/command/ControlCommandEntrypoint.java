/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.Entrypoint;

import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Strings.repeat;
import static java.lang.String.format;

/**
 * An entrypoint for running a {@link ControlCommand} against a list of {@link Target} clusters.
 *
 * This {@link Entrypoint} does not listen to the entrypoint latch, it will return as soon as the
 * command has finished.
 */
class ControlCommandEntrypoint implements Entrypoint {

  private static final Logger log = LoggerFactory.getLogger(ControlCommandEntrypoint.class);

  private final ControlCommand controlCommand;
  private final Namespace options;
  private final List<Target> targets;
  private final PrintStream out;

  ControlCommandEntrypoint(final ControlCommand controlCommand, final Namespace options,
                           final List<Target> targets, final PrintStream out) {
    this.controlCommand = controlCommand;
    this.options = options;
    this.targets = targets;
    this.out = out;
  }

  /**
   * Runs the {@link ControlCommand} against all {@link Target}s.
   *
   * @param runLatch ignored
   * @return 0 if the command was executed successfully for all target, 1 otherwise.
   */
  @Override
  public int enter(CountDownLatch runLatch) throws Exception {
    boolean successful = true;

    // Execute the control command over each target cluster
    for (final Target target : targets) {
      if (targets.size() > 1) {
        final String header = format("%s (%s)", target.name, target.endpoint);
        out.println(header);
        out.println(repeat("-", header.length()));
        out.flush();
      }

      try {
        successful &= controlCommand.runControl(options, target);
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
   * A target cluster identified by an endpoint string that can be used with a {@link
   * com.spotify.helios.service.Client}.
   */
  public static class Target {

    final String name;
    final String endpoint;

    Target(final String name, final String endpoint) {
      this.name = name;
      this.endpoint = endpoint;
    }
  }

}
