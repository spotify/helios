/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.cli.CliConfig;
import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.JobDescriptor;
import com.spotify.hermes.message.StatusCode;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class JobCreateCommand extends ControlCommand {

  private final Argument quietArg;
  private final Argument nameArg;
  private final Argument versionArg;
  private final Argument imageArg;
  private final Argument argsArg;

  public JobCreateCommand(
      final Subparser parser,
      final CliConfig cliConfig,
      final PrintStream out) {
    super(parser, cliConfig, out);

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print job id");

    nameArg = parser.addArgument("name")
        .help("Job name");

    versionArg = parser.addArgument("version")
        .help("Job version");

    imageArg = parser.addArgument("image")
        .help("Container image");

    argsArg = parser.addArgument("args")
        .nargs("*")
        .help("Command line arguments");
  }

  @Override int runControl(final Namespace options, final Client client)
      throws ExecutionException, InterruptedException {
    final List<String> command = options.getList(argsArg.getDest());
    final String imageIdentifier = options.getString(imageArg.getDest());
    final JobDescriptor descriptor = JobDescriptor.newBuilder()
        .setName(options.getString(nameArg.getDest()))
        .setVersion(options.getString(versionArg.getDest()))
        .setImage(imageIdentifier)
        .setCommand(command)
        .build();

    final boolean quiet = options.getBoolean(quietArg.getDest());

    if (!quiet) {
      out.println("Creating job: " + descriptor.toJsonString());
    }

    final StatusCode status = client.createJob(descriptor).get();
    if (status == StatusCode.OK) {
      if (!quiet) {
        out.println("Done.");
      }
      out.println(descriptor.getId());
      return 0;
    } else {
      if (!quiet) {
        out.println("Failed: " + status);
      }
      return 1;
    }
  }
}

