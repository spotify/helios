/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli;

import com.google.common.collect.Maps;

import com.spotify.helios.cli.command.ControlCommand;
import com.spotify.helios.cli.command.HostJobsCommand;
import com.spotify.helios.cli.command.HostListCommand;
import com.spotify.helios.cli.command.HostRegisterCommand;
import com.spotify.helios.cli.command.JobCreateCommand;
import com.spotify.helios.cli.command.JobDeployCommand;
import com.spotify.helios.cli.command.JobListCommand;
import com.spotify.helios.cli.command.JobUndeployCommand;
import com.spotify.helios.common.LoggingConfig;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

import org.json.JSONException;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

import static net.sourceforge.argparse4j.impl.Arguments.SUPPRESS;
import static net.sourceforge.argparse4j.impl.Arguments.fileType;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class CliParser {

  private final Namespace options;
  private final ControlCommand command;
  private final LoggingConfig loggingConfig;

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public CliParser(final CommandsConfigFactory commandsConfigFactory, final String... args)
      throws ArgumentParserException, JSONException, IOException {
    this(commandsConfigFactory, System.out, args);
  }

  private CliParser(final CommandsConfigFactory commandsConfigFactory,
                    final PrintStream out,
                    final String[] args)
      throws ArgumentParserException, IOException, JSONException {

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("sphelios")
        .defaultHelp(true)
        .description("Spotify Helios");

    addGlobalArgs(parser);

    final CliConfig cliConfig = CliConfig.fromUserConfig();

    commandsConfigFactory.get(parser).setupCommands(cliConfig, out);

    try {
      this.options = parser.parseArgs(args);
      this.command = (ControlCommand) options.get("command");
      this.loggingConfig = new LoggingConfig(options.getInt("verbose"),
                                             options.getBoolean("syslog"),
                                             (File) options.get("logconfig"),
                                             options.getBoolean("no_log_setup"));
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      throw e;
    }
  }

  public static void addGlobalArgs(final ArgumentParser parser) {
    final ArgumentGroup globalArgs = parser.addArgumentGroup("global options");

    globalArgs.addArgument("-v", "--verbose")
        .action(Arguments.count());

    globalArgs.addArgument("--syslog")
        .help("Log to syslog.")
        .action(storeTrue());

    globalArgs.addArgument("--logconfig")
        .type(fileType().verifyExists().verifyCanRead())
        .help("Logback configuration file.");

    globalArgs.addArgument("--no-log-setup")
        .action(storeTrue())
        .help(SUPPRESS);
  }

  public Namespace getNamespace() {
    return options;
  }

  public ControlCommand getCommand() {
    return command;
  }

  public LoggingConfig getLoggingConfig() {
    return loggingConfig;
  }

  public static CliParser createDefaultParser(final PrintStream out, final String[] args)
      throws ArgumentParserException, JSONException, IOException {

    return new CliParser(new CommandsConfigFactory() {
      @Override
      public CommandsConfig get(ArgumentParser parser) {
        return new DefaultCommands(parser);
      }
    }, out, args);
  }

  private interface CommandsConfigFactory {

    CommandsConfig get(ArgumentParser parser);
  }

  private static abstract class CommandsConfig {

    private final Subparsers commandParsers;

    private final Map<String, Subparser> subparserMap = Maps.newHashMap();

    private CommandsConfig(ArgumentParser parser) {
      commandParsers = parser.addSubparsers().title("commands");
    }

    protected Subparser p(final String name) {
      Subparser p = subparserMap.get(name);
      if (p == null) {
        p = commandParsers.addParser(name, true);
        subparserMap.put(name, p);
      }
      return p;
    }

    protected Subparser p(final Subparser subparser, final String name) {
      return subparser.addSubparsers().addParser(name, true);
    }

    abstract void setupCommands(CliConfig cliConfig, PrintStream out);
  }

  private static class DefaultCommands extends CommandsConfig {

    private DefaultCommands(ArgumentParser parser) {
      super(parser);
    }

    @Override
    public void setupCommands(final CliConfig cliConfig, final PrintStream out) {
      // Job commands
      final Subparser job = p("job");
      new JobListCommand(p(job, "list"), cliConfig, out);
      new JobCreateCommand(p(job, "create"), cliConfig, out);
      new JobDeployCommand(p(job, "deploy"), cliConfig, out);
      new JobUndeployCommand(p(job, "undeploy"), cliConfig, out);

      // Host commands
      final Subparser host = p("host");
      new HostListCommand(p(host, "list"), cliConfig, out);
      new HostJobsCommand(p(host, "jobs"), cliConfig, out);
      new HostRegisterCommand(p(host, "register"), cliConfig, out);
    }
  }
}
