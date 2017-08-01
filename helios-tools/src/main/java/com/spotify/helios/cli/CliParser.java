/*-
 * -\-\-
 * Helios Tools
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.cli;

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.spotify.helios.cli.command.CliCommand;
import com.spotify.helios.cli.command.DeploymentGroupCreateCommand;
import com.spotify.helios.cli.command.DeploymentGroupInspectCommand;
import com.spotify.helios.cli.command.DeploymentGroupListCommand;
import com.spotify.helios.cli.command.DeploymentGroupRemoveCommand;
import com.spotify.helios.cli.command.DeploymentGroupStatusCommand;
import com.spotify.helios.cli.command.DeploymentGroupStopCommand;
import com.spotify.helios.cli.command.DeploymentGroupWatchCommand;
import com.spotify.helios.cli.command.HostDeregisterCommand;
import com.spotify.helios.cli.command.HostListCommand;
import com.spotify.helios.cli.command.HostRegisterCommand;
import com.spotify.helios.cli.command.JobCreateCommand;
import com.spotify.helios.cli.command.JobDeployCommand;
import com.spotify.helios.cli.command.JobHistoryCommand;
import com.spotify.helios.cli.command.JobInspectCommand;
import com.spotify.helios.cli.command.JobListCommand;
import com.spotify.helios.cli.command.JobRemoveCommand;
import com.spotify.helios.cli.command.JobStartCommand;
import com.spotify.helios.cli.command.JobStatusCommand;
import com.spotify.helios.cli.command.JobStopCommand;
import com.spotify.helios.cli.command.JobUndeployCommand;
import com.spotify.helios.cli.command.JobWatchCommand;
import com.spotify.helios.cli.command.MasterListCommand;
import com.spotify.helios.cli.command.RollingUpdateCommand;
import com.spotify.helios.cli.command.VersionCommand;
import com.spotify.helios.common.LoggingConfig;
import com.spotify.helios.common.Version;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

public class CliParser {

  private static final String NAME_AND_VERSION = "Spotify Helios CLI " + Version.POM_VERSION;
  private static final String TESTED_DOCKER_VERSION = "1.12.1";
  private static final String HELP_ISSUES =
      "Report improvements/bugs at https://github.com/spotify/helios/issues";
  private static final String HELP_WIKI =
      "For documentation see https://github.com/spotify/helios/tree/master/docs";

  private final Namespace options;
  private final CliCommand command;
  private final LoggingConfig loggingConfig;
  private final Subparsers commandParsers;
  private final CliConfig cliConfig;
  private final List<Target> targets;
  private final String username;
  private boolean json;

  public CliParser(final String... args)
      throws ArgumentParserException, IOException, URISyntaxException {

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("helios")
        .defaultHelp(true)
        .version(format("%s%nTested on Docker %s", NAME_AND_VERSION, TESTED_DOCKER_VERSION))
        .description(format("%s%n%n%s%n%s", NAME_AND_VERSION, HELP_ISSUES, HELP_WIKI));

    cliConfig = CliConfig.fromUserConfig(System.getenv());

    final GlobalArgs globalArgs = addGlobalArgs(parser, cliConfig, true);

    commandParsers = parser.addSubparsers()
        .metavar("COMMAND")
        .title("commands");

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

    this.command = options.get("command");
    final String username = options.getString(globalArgs.usernameArg.getDest());
    this.username = (username == null) ? cliConfig.getUsername() : username;
    this.json = equal(options.getBoolean(globalArgs.jsonArg.getDest()), true);
    this.loggingConfig = new LoggingConfig(options.getInt(globalArgs.verbose.getDest()),
        false, null,
        options.getBoolean(globalArgs.noLogSetup.getDest()));

    // Merge domains and explicit endpoints into master endpoints
    final List<String> explicitEndpoints = options.getList(globalArgs.masterArg.getDest());
    final List<String> domains = options.getList(globalArgs.domainsArg.getDest());
    final String srvName = options.getString(globalArgs.srvNameArg.getDest());

    // Order of target precedence:
    // 1. endpoints from command line
    // 2. domains from command line
    // 3. endpoints from config file
    // 4. domains from config file

    // TODO (dano): complex, refactor and unit test it
    this.targets = computeTargets(parser, explicitEndpoints, domains, srvName);
  }

  private List<Target> computeTargets(final ArgumentParser parser,
                                      final List<String> explicitEndpoints,
                                      final List<String> domainsArguments,
                                      final String srvName) {

    if (explicitEndpoints != null && !explicitEndpoints.isEmpty()) {
      final List<Target> targets = Lists.newArrayListWithExpectedSize(explicitEndpoints.size());
      for (final String endpoint : explicitEndpoints) {
        targets.add(Target.from(URI.create(endpoint)));
      }
      return targets;
    } else if (domainsArguments != null && !domainsArguments.isEmpty()) {
      final Iterable<String> domains = parseDomains(domainsArguments);
      return Target.from(srvName, domains);
    } else if (!cliConfig.getMasterEndpoints().isEmpty()) {
      final List<URI> cliConfigMasterEndpoints = cliConfig.getMasterEndpoints();
      final List<Target> targets = Lists.newArrayListWithExpectedSize(
          cliConfigMasterEndpoints.size());
      for (final URI endpoint : cliConfigMasterEndpoints) {
        targets.add(Target.from(endpoint));
      }
      return targets;
    } else if (!cliConfig.getDomainsString().isEmpty()) {
      final Iterable<String> domains = parseDomainsString(cliConfig.getDomainsString());
      return Target.from(srvName, domains);
    }

    handleError(parser, new ArgumentParserException(
        "no masters specified.  Use the -z or -d option to specify which helios "
        + "cluster/master to connect to", parser));
    return ImmutableList.of();
  }

  private Iterable<String> parseDomains(final List<String> domainStrings) {
    final Set<String> domains = Sets.newLinkedHashSet();
    for (final String s : domainStrings) {
      addAll(domains, parseDomainsString(s));
    }
    return domains;
  }

  private Iterable<String> parseDomainsString(final String domainsString) {
    return filter(asList(domainsString.split(",")), not(equalTo("")));
  }

  private void setupCommands() {
    // Job commands
    new JobCreateCommand(parse("create"));
    new JobRemoveCommand(parse("remove"));
    new JobInspectCommand(parse("inspect"));
    new JobDeployCommand(parse("deploy"));
    new JobUndeployCommand(parse("undeploy"));
    new JobStartCommand(parse("start"));
    new JobStopCommand(parse("stop"));
    new JobHistoryCommand(parse("history"));
    new JobListCommand(parse("jobs"));
    new JobStatusCommand(parse("status"));
    new JobWatchCommand(parse("watch"));

    // Host commands
    new HostListCommand(parse("hosts"));
    new HostRegisterCommand(parse("register"));
    new HostDeregisterCommand(parse("deregister"));

    // Master commands
    new MasterListCommand(parse("masters"));

    // Deployment group commands
    new DeploymentGroupCreateCommand(parse("create-deployment-group"));
    new DeploymentGroupRemoveCommand(parse("remove-deployment-group"));
    new DeploymentGroupListCommand(parse("list-deployment-groups"));
    new DeploymentGroupInspectCommand(parse("inspect-deployment-group"));
    new DeploymentGroupStatusCommand(parse("deployment-group-status"));
    new DeploymentGroupWatchCommand(parse("watch-deployment-group"));
    new RollingUpdateCommand(parse("rolling-update"));
    new DeploymentGroupStopCommand(parse("stop-deployment-group"));

    // Version Command
    final Subparser version = parse("version").help("print version of master and client");
    new VersionCommand(version);
  }

  /**
   * Use this instead of calling parser.handle error directly. This will print a header with
   * links to jira and documentation before the standard error message is printed.
   *
   * @param parser the parser which will print the standard error message
   * @param ex     the exception that will be printed
   */
  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  private void handleError(ArgumentParser parser, ArgumentParserException ex) {
    System.err.println("# " + HELP_ISSUES);
    System.err.println("# " + HELP_WIKI);
    System.err.println("# ---------------------------------------------------------------");
    parser.handleError(ex);
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
    private final Argument domainsArg;
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

      domainsArg = addArgument("-d", "--domains")
          .setDefault(new ArrayList<>())
          .action(append())
          .help("domains");

      srvNameArg = addArgument("--srv-name")
          .setDefault(cliConfig.getSrvName())
          .help("master srv name");

      usernameArg = addArgument("-u", "--username")
          .setDefault(System.getProperty("user.name"))
          .help("username");

      verbose = addArgument("-v", "--verbose")
          .action(Arguments.count());

      addArgument("--version")
          .action(Arguments.version())
          .help("print version");

      jsonArg = addArgument("--json")
          .action(storeTrue())
          .help("json output");

      noLogSetup = addArgument("--no-log-setup")
          .action(storeTrue())
          .help(SUPPRESS);

      // note: because of the way the HeliosClient is constructed, these next arguments are
      // read indirectly in cli/Utils.java:

      addArgument("-k", "--insecure")
          .action(storeTrue())
          .help("Disables hostname verification of HTTPS connections. "
                + "Similar to 'curl -k'. "
                + "Useful when using -z flag to connect directly to a master using HTTPS which "
                + "presents a certificate whose subject does not match the actual hostname.");

      // for http-timeout and retry-timeout, do not set a default value in the argument, so that
      // envrionment variables can be inspected in the Utils client factory.
      addArgument("--http-timeout")
          .type(Integer.class)
          .help("Timeout (in seconds) for each HTTP/S request to the master. "
                + "If this flag is not set, the value in the environment variable "
                + Utils.HTTP_TIMEOUT_ENV_VAR + " will be used. "
                + "If this environment variable is not set, then the default is "
                + Utils.DEFAULT_HTTP_TIMEOUT_SECS + " seconds.");

      addArgument("--retry-timeout")
          .type(Integer.class)
          .help("Total timeout (in seconds) for all of the requests that helios makes to the "
                + "master. If an individual request fails, helios will retry the request again "
                + "until successful or until this timeout elapses. "
                + "If this flag is not set, the value in the environment variable "
                + Utils.TOTAL_TIMEOUT_ENV_VAR + " will be used. "
                + "If this environment variable is not set, then the default is "
                + Utils.DEFAULT_TOTAL_TIMEOUT_SECS + " seconds.");
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

  public CliCommand getCommand() {
    return command;
  }

  public LoggingConfig getLoggingConfig() {
    return loggingConfig;
  }

  private Subparser parse(final String name) {
    return parse(commandParsers, name);
  }

  private Subparser parse(final Subparsers subparsers, final String name) {
    final Subparser subparser = subparsers.addParser(name, true);
    addGlobalArgs(subparser, cliConfig);
    return subparser;
  }
}
