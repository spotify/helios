/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.Maps;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.JobValidator;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.protocol.CreateJobResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;

import static java.util.regex.Pattern.compile;
import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class JobCreateCommand extends ControlCommand {

  private static final JobValidator JOB_VALIDATOR = new JobValidator();

  private final Argument quietArg;
  private final Argument nameArg;
  private final Argument versionArg;
  private final Argument imageArg;
  private final Argument envArg;
  private final Argument argsArg;
  private final Argument portArg;
  private final Argument namelessService;

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

    portArg = parser.addArgument("-p", "--port")
        .action(append())
        .setDefault(new ArrayList<String>())
        .nargs("+")
        .help("Port mapping. Specify a name and a single port (e.g. \"http=8080\") number for " +
              "dynamic port mapping and a name=private:public tuple (e.g. \"http=8080:80\") for " +
              "static port mapping. E.g., foo=4711 will map the internal port 4711 of the job to " +
              "an arbitrary external port on the host. Specifying foo=4711:80 will map internal " +
              "port 4711 of the job to port 80 on the host.");

    namelessService = parser.addArgument("-r", "--service")
        .help("Set the Nameless service name for the job");

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
    final List<List<String>> portSpecLists = options.getList(portArg.getDest());

    final List<List<String>> envList = options.getList(envArg.getDest());
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

    final Map<String, PortMapping> ports = Maps.newHashMap();
    for (final List<String> specs : portSpecLists) {
      for (final String spec : specs) {
        final Matcher matcher = compile("(?<n>\\w+)=(?<i>\\d+)(:?(?<e>\\d+))").matcher(spec);
        if (!matcher.matches()) {
          throw new IllegalArgumentException("Bad port mapping: " + spec);
        }

        final String name = matcher.group("n");
        final int internal = Integer.parseInt(matcher.group("i"));
        final Integer external = nullOrInteger(matcher.group("e"));

        if (ports.containsKey(name)) {
          throw new IllegalArgumentException("Duplicate port mapping: " + name);
        }

        ports.put(name, PortMapping.of(internal, external));
      }
    }

    final Job job = Job.newBuilder()
        .setName(options.getString(nameArg.getDest()))
        .setVersion(options.getString(versionArg.getDest()))
        .setImage(imageIdentifier)
        .setCommand(command)
        .setEnv(env)
        .setPorts(ports)
        .setService(options.getString(namelessService.getDest()))
        .build();

    final Collection<String> errors = JOB_VALIDATOR.validate(job);
    if (!errors.isEmpty()) {
      // TODO: nicer output
      throw new IllegalArgumentException("Bad job definition: " + errors);
    }

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

  private Integer nullOrInteger(final String s) {
    return s == null ? null : Integer.valueOf(s);
  }
}

