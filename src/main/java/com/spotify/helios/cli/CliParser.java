/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli;

import com.spotify.helios.cli.command.ControlCommand;
import com.spotify.helios.cli.command.HostDeregisterCommand;
import com.spotify.helios.cli.command.HostJobsCommand;
import com.spotify.helios.cli.command.HostListCommand;
import com.spotify.helios.cli.command.HostRegisterCommand;
import com.spotify.helios.cli.command.HostStatusCommand;
import com.spotify.helios.cli.command.JobCreateCommand;
import com.spotify.helios.cli.command.JobDeployCommand;
import com.spotify.helios.cli.command.JobListCommand;
import com.spotify.helios.cli.command.JobRemoveCommand;
import com.spotify.helios.cli.command.JobStatusCommand;
import com.spotify.helios.cli.command.JobStopCommand;
import com.spotify.helios.cli.command.JobUndeployCommand;
import com.spotify.helios.cli.command.MasterListCommand;
import com.spotify.helios.common.LoggingConfig;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

import org.json.JSONException;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.spotify.helios.cli.Target.targetsFrom;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static net.sourceforge.argparse4j.impl.Arguments.SUPPRESS;
import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class CliParser {

  private static final List<String> DEFAULT_MASTER_ENDPOINTS = asList("tcp://localhost:5800");

  private final Namespace options;
  private final ControlCommand command;
  private final LoggingConfig loggingConfig;
  private final Subparsers commandParsers;
  private final CliConfig cliConfig;
  private final List<Target> targets;
  private final String username;
  private boolean json;

  public CliParser(final String... args)
      throws ArgumentParserException, IOException, JSONException {

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("helios")
        .defaultHelp(true)
        .description("Spotify Helios CLI");

    cliConfig = CliConfig.fromUserConfig();

    final GlobalArgs globalArgs = addGlobalArgs(parser, cliConfig);

    commandParsers = parser.addSubparsers().title("commands");

    setupCommands();

    try {
      this.options = parser.parseArgs(args);
      this.command = (ControlCommand) options.get("command");
      final String username = options.getString(globalArgs.usernameArg.getDest());
      this.username = (username == null) ? cliConfig.getUsername() : username;
      this.json = options.getBoolean(globalArgs.jsonArg.getDest());
      this.loggingConfig = new LoggingConfig(options.getInt(globalArgs.verbose.getDest()),
                                             false, null,
                                             options.getBoolean(globalArgs.noLogSetup.getDest()));
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      throw e;
    }

    // Merge sites and explicit endpoints into master endpoints
    final List<String> explicitEndpoints = options.getList(globalArgs.masterArg.getDest());
    final String sitesArgument = options.getString(globalArgs.sitesArg.getDest());
    final String srvName = options.getString(globalArgs.srvNameArg.getDest());

    // Order of target precedence:
    // 1. endpoints from command line
    // 3. sites from command line
    // 2. endpoints from config file
    // 4. sites from config file
    // 3. default (localhost)

    // TODO (dano): this is kind of complex, make sure it matches the defaults in the help and maybe factor out and unit test it

    if (explicitEndpoints != null && !explicitEndpoints.isEmpty()) {
      this.targets = targetsFrom(explicitEndpoints);
    } else if (sitesArgument != null && !sitesArgument.isEmpty()) {
      final Iterable<String> sites = parseSitesString(sitesArgument);
      this.targets = targetsFrom(srvName, sites);
    } else if (!cliConfig.getMasterEndpoints().isEmpty()) {
      this.targets = targetsFrom(cliConfig.getMasterEndpoints());
    } else if (!cliConfig.getSitesString().isEmpty()) {
      final Iterable<String> sites = parseSitesString(cliConfig.getSitesString());
      this.targets = targetsFrom(srvName, sites);
    } else {
      this.targets = targetsFrom(DEFAULT_MASTER_ENDPOINTS);
    }

    if (targets.isEmpty()) {
      parser.handleError(new ArgumentParserException("no masters specified", parser));
    }
  }

  private Iterable<String> parseSitesString(final String sitesString) {
    return filter(asList(sitesString.split(",")), not(equalTo("")));
  }

  private void setupCommands() {
    // Job commands
    final Subparsers job = p("job").help("job commands")
        .addSubparsers().title("job commands").metavar("COMMAND").help("additional help");
    new JobListCommand(p(job, "list"));
    new JobCreateCommand(p(job, "create"));
    new JobDeployCommand(p(job, "deploy"));
    new JobUndeployCommand(p(job, "undeploy"));
    new JobStopCommand(p(job, "stop"));
    new JobRemoveCommand(p(job, "remove"));
    new JobStatusCommand(p(job, "status"));

    // Host commands
    final Subparsers host = p("host").help("host commands")
        .addSubparsers().title("host commands").metavar("COMMAND").help("additional help");
    new HostListCommand(p(host, "list"));
    new HostJobsCommand(p(host, "jobs"));
    new HostRegisterCommand(p(host, "register"));
    new HostDeregisterCommand(p(host, "delete"));
    new HostStatusCommand(p(host, "status"));

    // Master Commands
    final Subparsers master = p("master").help("master commands")
        .addSubparsers().title("master commands").metavar("COMMAND").help("additional help");
    new MasterListCommand(p(master, "list"));
  }

  public List<Target> getTargets() {
    return targets;
  }

  public String getUsername() {
    return username;
  }

  public boolean getJson() {
    return json;
  }

  private static List<String> getDefaultMasterEndpoints(final CliConfig cliConfig) {
    final List<String> defaultMaster;
    if (cliConfig.getSites().isEmpty()) {
      if (!cliConfig.getMasterEndpoints().isEmpty()) {
        defaultMaster = cliConfig.getMasterEndpoints();
      } else {
        defaultMaster = DEFAULT_MASTER_ENDPOINTS;
      }
    } else {
      defaultMaster = emptyList();
    }
    return defaultMaster;
  }

  private static class GlobalArgs {

    private final Argument masterArg;
    private final Argument sitesArg;
    private final Argument srvNameArg;
    private final Argument usernameArg;
    private final Argument verbose;
    private final Argument noLogSetup;
    private final Argument jsonArg;

    GlobalArgs(final ArgumentParser parser, final CliConfig cliConfig) {
      final ArgumentGroup globalArgs = parser.addArgumentGroup("global options");

      masterArg = globalArgs.addArgument("-z", "--master")
          .action(append())
          .help(format("master endpoint (default: %s)", getDefaultMasterEndpoints(cliConfig)));

      sitesArg = globalArgs.addArgument("-s", "--sites")
          .help(format("sites (default: %s)", cliConfig.getSitesString()));

      srvNameArg = globalArgs.addArgument("--srv-name")
          .setDefault(cliConfig.getSrvName())
          .help("master srv name");

      usernameArg = globalArgs.addArgument("-u", "--username")
          .setDefault(System.getProperty("user.name"))
          .help("username");

      verbose = globalArgs.addArgument("-v", "--verbose")
          .action(Arguments.count());

      jsonArg = globalArgs.addArgument("--json")
          .action(storeTrue())
          .help("json output");

      noLogSetup = globalArgs.addArgument("--no-log-setup")
          .action(storeTrue())
          .help(SUPPRESS);
    }
  }

  private GlobalArgs addGlobalArgs(final ArgumentParser parser, final CliConfig cliConfig) {
    return new GlobalArgs(parser, cliConfig);
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

  private Subparser p(final String name) {
    return p(commandParsers, name);
  }

  private Subparser p(final Subparsers subparsers, final String name) {
    final Subparser subparser = subparsers.addParser(name, true);
    addGlobalArgs(subparser, cliConfig);
    return subparser;
  }
}
