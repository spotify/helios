/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.spotify.helios.cli.command.ControlCommand;
import com.spotify.helios.cli.command.HostDeregisterCommand;
import com.spotify.helios.cli.command.HostJobsCommand;
import com.spotify.helios.cli.command.HostListCommand;
import com.spotify.helios.cli.command.HostRegisterCommand;
import com.spotify.helios.cli.command.HostStatusCommand;
import com.spotify.helios.cli.command.JobCreateCommand;
import com.spotify.helios.cli.command.JobDeployCommand;
import com.spotify.helios.cli.command.JobHistoryCommand;
import com.spotify.helios.cli.command.JobListCommand;
import com.spotify.helios.cli.command.JobRemoveCommand;
import com.spotify.helios.cli.command.JobStartCommand;
import com.spotify.helios.cli.command.JobStatusCommand;
import com.spotify.helios.cli.command.JobStopCommand;
import com.spotify.helios.cli.command.JobUndeployCommand;
import com.spotify.helios.cli.command.JobWatchCommand;
import com.spotify.helios.cli.command.MasterListCommand;
import com.spotify.helios.common.LoggingConfig;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.FeatureControl;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Objects.equal;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Iterables.filter;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static net.sourceforge.argparse4j.impl.Arguments.SUPPRESS;
import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class CliParser {

  private static final String HELP_JIRA =
      "Report improvements/bugs at https://jira.spotify.net/browse/HEL";
  private static final String HELP_WIKI =
      "For documentation see https://wiki.spotify.net/wiki/Helios";

  private final Namespace options;
  private final ControlCommand command;
  private final LoggingConfig loggingConfig;
  private final Subparsers commandParsers;
  private final CliConfig cliConfig;
  private final List<Target> targets;
  private final String username;
  private boolean json;

  public CliParser(final String... args)
      throws ArgumentParserException, IOException {

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("helios")
        .defaultHelp(true)
        .description(format("%s%n%n%s%n%s", "Spotify Helios CLI", HELP_JIRA, HELP_WIKI));

    cliConfig = CliConfig.fromUserConfig();

    final GlobalArgs globalArgs = addGlobalArgs(parser, cliConfig, true);

    commandParsers = parser.addSubparsers().title("commands");

    setupCommands();

    if (args.length == 0) {
      parser.printHelp();
      throw new ArgumentParserException(parser);
    }

    try {
      this.options = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      handleError(parser, e);
      throw e;
    }

    this.command = (ControlCommand) options.get("command");
    final String username = options.getString(globalArgs.usernameArg.getDest());
    this.username = (username == null) ? cliConfig.getUsername() : username;
    this.json = equal(options.getBoolean(globalArgs.jsonArg.getDest()), true);
    this.loggingConfig = new LoggingConfig(options.getInt(globalArgs.verbose.getDest()),
                                           false, null,
                                           options.getBoolean(globalArgs.noLogSetup.getDest()));

    // Merge sites and explicit endpoints into master endpoints
    final List<String> explicitEndpoints = options.getList(globalArgs.masterArg.getDest());
    final List<String> sitesArguments = options.getList(globalArgs.sitesArg.getDest());
    final String srvName = options.getString(globalArgs.srvNameArg.getDest());

    // Order of target precedence:
    // 1. endpoints from command line
    // 2. sites from command line
    // 3. endpoints from config file
    // 4. sites from config file

    // TODO (dano): this is kind of complex, make sure it matches the defaults in the help and maybe factor out and unit test it
    this.targets = computeTargets(parser, explicitEndpoints, sitesArguments, srvName);
  }

  private List<Target> computeTargets(final ArgumentParser parser,
                                      final List<String> explicitEndpoints,
                                      final List<String> sitesArguments, final String srvName) {

    if (explicitEndpoints != null && !explicitEndpoints.isEmpty()) {
      final List<URI> explicitEndpointURIs = Lists.newArrayList();
      for (final String endpoint : explicitEndpoints) {
        explicitEndpointURIs.add(URI.create(endpoint));
      }
      return asList(Target.from(explicitEndpointURIs));
    } else if (sitesArguments != null && !sitesArguments.isEmpty()) {
      final Iterable<String> sites = parseSitesStrings(sitesArguments);
      return Target.from(srvName, sites);
    } else if (!cliConfig.getMasterEndpoints().isEmpty()) {
      return asList(Target.from(cliConfig.getMasterEndpoints()));
    } else if (!cliConfig.getSitesString().isEmpty()) {
      final Iterable<String> sites = parseSitesString(cliConfig.getSitesString());
      return Target.from(srvName, sites);
    }

    handleError(parser, new ArgumentParserException(
        "no masters specified.  Use the -z or -s option to specify which helios "
        + "cluster/master to connect to", parser));
    return ImmutableList.of();
  }

  private Iterable<String> parseSitesStrings(final List<String> sitesStrings) {
    final Set<String> sites = Sets.newLinkedHashSet();
    for (final String s : sitesStrings) {
      addAll(sites, parseSitesString(s));
    }
    return sites;
  }

  private Iterable<String> parseSitesString(final String sitesString) {
    return filter(asList(sitesString.split(",")), not(equalTo("")));
  }

  private void setupCommands() {
    // Job commands
    final Subparsers job = p("job").help("job commands")
        .addSubparsers().title("job commands").metavar("COMMAND").help("additional help");
    new JobCreateCommand(p(job, "create"));
    new JobDeployCommand(p(job, "deploy"));
    new JobHistoryCommand(p(job, "history"));
    new JobListCommand(p(job, "list"));
    new JobRemoveCommand(p(job, "remove"));
    new JobStartCommand(p(job, "start"));
    new JobStatusCommand(p(job, "status"));
    new JobStopCommand(p(job, "stop"));
    new JobUndeployCommand(p(job, "undeploy"));
    new JobWatchCommand(p(job, "watch"));

    // Host commands
    final Subparsers host = p("host").help("host commands")
        .addSubparsers().title("host commands").metavar("COMMAND").help("additional help");
    new HostDeregisterCommand(p(host, "deregister"));
    new HostJobsCommand(p(host, "jobs"));
    new HostListCommand(p(host, "list"));
    new HostRegisterCommand(p(host, "register"));
    new HostStatusCommand(p(host, "status"));

    // Master Commands
    final Subparsers master = p("master").help("master commands")
        .addSubparsers().title("master commands").metavar("COMMAND").help("additional help");
    new MasterListCommand(p(master, "list"));
  }

  /**
   * Use this instead of calling parser.handle error directly. This will print a header with
   * links to jira and documentation before the standard error message is printed.
   * @param parser the parser which will print the standard error message
   * @param e the exception that will be printed
   */
  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  private void handleError(ArgumentParser parser, ArgumentParserException e) {
    System.err.println("# " + HELP_JIRA);
    System.err.println("# " + HELP_WIKI);
    System.err.println("# ---------------------------------------------------------------");
    parser.handleError(e);
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

  private static class GlobalArgs {

    private final Argument masterArg;
    private final Argument sitesArg;
    private final Argument srvNameArg;
    private final Argument usernameArg;
    private final Argument verbose;
    private final Argument noLogSetup;
    private final Argument jsonArg;

    private final ArgumentGroup globalArgs;
    private final boolean topLevel;


    GlobalArgs(final ArgumentParser parser, final CliConfig cliConfig) {
      this(parser, cliConfig, false);
    }

    GlobalArgs(final ArgumentParser parser, final CliConfig cliConfig, final boolean topLevel) {
      this.globalArgs = parser.addArgumentGroup("global options");
      this.topLevel = topLevel;

      masterArg = addArgument("-z", "--master")
          .action(append())
          .help("master endpoints");

      sitesArg = addArgument("-s", "--sites")
          .action(append())
          .help("sites");

      srvNameArg = addArgument("--srv-name")
          .setDefault(cliConfig.getSrvName())
          .help("master srv name");

      usernameArg = addArgument("-u", "--username")
          .setDefault(System.getProperty("user.name"))
          .help("username");

      verbose = addArgument("-v", "--verbose")
          .action(Arguments.count());

      jsonArg = addArgument("--json")
          .action(storeTrue())
          .help("json output");

      noLogSetup = addArgument("--no-log-setup")
          .action(storeTrue())
          .help(SUPPRESS);
    }

    private Argument addArgument(final String... nameOrFlags) {
      final FeatureControl defaultControl = topLevel ? null : SUPPRESS;
      return globalArgs.addArgument(nameOrFlags).setDefault(defaultControl);
    }
  }

  private GlobalArgs addGlobalArgs(final ArgumentParser parser, final CliConfig cliConfig) {
    return new GlobalArgs(parser, cliConfig);
  }

  private GlobalArgs addGlobalArgs(final ArgumentParser parser, final CliConfig cliConfig,
                                   final boolean topLevel) {
    return new GlobalArgs(parser, cliConfig, topLevel);
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
