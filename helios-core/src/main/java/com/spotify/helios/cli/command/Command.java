/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.Entrypoint;
import com.spotify.helios.cli.CliConfig;
import com.spotify.helios.cli.Parser;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;

public abstract class Command {

  protected final CliConfig cliConfig;
  protected final PrintStream out;

  Command(final Subparser parser, final CliConfig cliConfig, final PrintStream out) {
    this.cliConfig = cliConfig;
    this.out = out;

    parser
        .setDefault("command", this)
        .defaultHelp(true);

    Parser.addGlobalArgs(parser);
  }

  public abstract Entrypoint getEntrypoint(Namespace options);
}
