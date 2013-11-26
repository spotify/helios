/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.Maps;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.protocol.CreateJobResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class JobCreateCommand extends ControlCommand {

  private final Argument quietArg;
  private final Argument nameArg;
  private final Argument versionArg;
  private final Argument imageArg;
  private final Argument envArg;
  private final Argument argsArg;

  public JobCreateCommand(final Subparser parser) {
    super(parser);

    parser.help("create a job");

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print job id");

    nameArg = parser.addArgument("name")
        .help("Job name");

    versionArg = parser.addArgument("version")
        .help("Job version");

    imageArg = parser.addArgument("image")
        .help("Container image");

    envArg = parser.addArgument("--env")
        .action(append())
        .setDefault(new ArrayList<String>())
        .nargs("+")
        .help("Environment variables");

    argsArg = parser.addArgument("args")
        .nargs("*")
        .help("Command line arguments");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {
    final List<String> command = options.getList(argsArg.getDest());
    final String imageIdentifier = options.getString(imageArg.getDest());

    final boolean quiet = options.getBoolean(quietArg.getDest());

    final List<List<String>> envList = options.getList("env");
    final Map<String, String> env = Maps.newHashMap();
    if (env != null) {
      for (final List<String> group : envList) {
        for (final String s : group) {
          final String[] parts = s.split("=", 2);
          if (parts.length != 2) {
            throw new IllegalArgumentException("Bad environment variable: " + s);
          }
          env.put(parts[0], parts[1]);
        }
      }
    }

    final Job job = Job.newBuilder()
        .setName(options.getString(nameArg.getDest()))
        .setVersion(options.getString(versionArg.getDest()))
        .setImage(imageIdentifier)
        .setCommand(command)
        .setEnv(env)
        .build();

    if (!quiet) {
      out.println("Creating job: " + job.toJsonString());
    }

    final CreateJobResponse status = client.createJob(job).get();
    if (status.getStatus() == CreateJobResponse.Status.OK) {
      if (!quiet) {
        out.println("Done.");
      }
      out.println(job.getId());
      return 0;
    } else {
      if (!quiet) {
        out.println("Failed: " + status);
      }
      return 1;
    }
  }
}

