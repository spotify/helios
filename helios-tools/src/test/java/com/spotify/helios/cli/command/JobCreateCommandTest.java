package com.spotify.helios.cli.command;

import com.spotify.helios.cli.CliParser;
import com.spotify.helios.cli.Target;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Version;

import junit.framework.TestCase;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class JobCreateCommandTest extends TestCase {

  public void testRun() throws Exception {
    final String[] args = {"create", "-d", "fake-domain", "--file", "job_config.json",
                           "test-job:0.0.1",
                           "registry:80/spotify/test-image:0.0.1-ca65ec3"};
    final CliParser cliParser = new CliParser(args);
    final Namespace namespace = cliParser.getNamespace();
    final Target target = cliParser.getTargets().get(0);
    final String username = cliParser.getUsername();
    final boolean json = cliParser.getJson();
    final HeliosClient client = HeliosClient.newBuilder()
        .setEndpointSupplier(target.getEndpointSupplier())
        .setUser(username)
        .build();
    final PrintStream out = new PrintStream(new ByteArrayOutputStream());
//    final PrintStream err = new PrintStream(new ByteArrayOutputStream());

    final String NAME_AND_VERSION = "Spotify Helios CLI " + Version.POM_VERSION;
    final String HELP_ISSUES =
        "Report improvements/bugs at https://github.com/spotify/helios/issues";
    final String HELP_WIKI =
        "For documentation see https://github.com/spotify/helios/tree/master/docs";

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("helios")
        .defaultHelp(true)
        .version(NAME_AND_VERSION)
        .description(String.format("%s%n%n%s%n%s", NAME_AND_VERSION, HELP_ISSUES, HELP_WIKI));
    final Subparsers commandParsers = parser.addSubparsers().metavar("COMMAND").title("commands");
    final String name = "create";
    final Subparser subparser = commandParsers.addParser(name, true);
    final JobCreateCommand jobCreateCommand = new JobCreateCommand(subparser);
    final int run = jobCreateCommand.run(namespace, client, out, json);
  }
}