/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.JobValidator;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.protocol.CreateJobResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.fromNullable;
import static com.spotify.helios.common.descriptors.PortMapping.TCP;
import static com.spotify.helios.common.descriptors.ServiceEndpoint.HTTP;
import static java.util.regex.Pattern.compile;
import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.fileType;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class JobCreateCommand extends ControlCommand {

  private static final JobValidator JOB_VALIDATOR = new JobValidator();

  private final Argument quietArg;
  private final Argument idArg;
  private final Argument imageArg;
  private final Argument envArg;
  private final Argument argsArg;
  private final Argument portArg;
  private final Argument registrationArg;
  private final Argument fileArg;
  private final Argument templateArg;
  private final Argument volumeArg;

  public JobCreateCommand(final Subparser parser) {
    super(parser);

    parser.help("create a job");

    fileArg = parser.addArgument("-f", "--file")
        .type(fileType().acceptSystemIn())
        .help("Job configuration file. Options specified on the command line will be merged with" +
              "the contents of this file. Cannot be used together with -t/--template.");

    templateArg = parser.addArgument("-t", "--template")
        .help("Template job id. The new job will be based on this job. Cannot be used together " +
              "with -f/--file.");

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print job id");

    idArg = parser.addArgument("id")
        .nargs("?")
        .help("Job name:version[:hash]");

    imageArg = parser.addArgument("image")
        .nargs("?")
        .help("Container image");

    envArg = parser.addArgument("--env")
        .action(append())
        .setDefault(new ArrayList<String>())
        .nargs("+")
        .help("Environment variables");

    portArg = parser.addArgument("-p", "--port")
        .action(append())
        .setDefault(new ArrayList<String>())
        .help("Port mapping. Specify an endpoint name and a single port (e.g. \"http=8080\") for " +
              "dynamic port mapping and a name=private:public tuple (e.g. \"http=8080:80\") for " +
              "static port mapping. E.g., foo=4711 will map the internal port 4711 of the " +
              "container to an arbitrary external port on the host. Specifying foo=4711:80 " +
              "will map internal port 4711 of the container to port 80 on the host. The " +
              "protocol will be TCP by default. For UDP, add /udp. E.g. quic=80/udp or " +
              "dns=53:53/udp. The endpoint name can be used when specify service registration " +
              "using -r/--register.");

    registrationArg = parser.addArgument("-r", "--register")
        .action(append())
        .setDefault(new ArrayList<String>())
        .nargs("+")
        .help("Service discovery registration. Specify a service name, the port name and a " +
              "protocol on the format service/protocol=port. E.g. -r website/tcp=http will " +
              "register the port named http with the protocol tcp. Protocol is optional and " +
              "default is http. If there is only one port mapping this will be used by" +
              "default and it will be enough to specify only the service name, e.g." +
              "-r wordpress.");

    volumeArg = parser.addArgument("--volume")
        .action(append())
        .setDefault(new ArrayList<String>())
        .help("Container volumes. Specify either a single path to create a data volume, " +
              "or a source path and a container path to mount a file or directory from the host. " +
              "The container path can be suffixed with \"rw\" or \"ro\" to create a read-write " +
              "or read-only volume, respectively. Format: [host-path]:[container-path]:[rw|ro].");

    argsArg = parser.addArgument("args")
        .nargs("*")
        .help("Command line arguments");
  }

  @Override
  int run(Namespace options, HeliosClient client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException, IOException {

    final boolean quiet = options.getBoolean(quietArg.getDest());

    final Job.Builder builder;

    final String id = options.getString(idArg.getDest());
    final String imageIdentifier = options.getString(imageArg.getDest());

    // Read job configuration from file

    // TODO (dano): look for e.g. Heliosfile in cwd by default?

    final String templateJobId = options.getString(templateArg.getDest());
    final File file = (File) options.get(fileArg.getDest());

    if (file != null && templateJobId != null) {
      throw new IllegalArgumentException("Please use only one of -t/--template and -f/--file");
    }

    if (file != null && file.exists()) {
      if (!file.isFile() || !file.canRead()) {
        throw new IllegalArgumentException("Cannot read file " + file);
      }
      final byte[] bytes = Files.readAllBytes(file.toPath());
      final String config = new String(bytes, UTF_8);
      final Job job = Json.read(config, Job.class);
      builder = job.toBuilder();
     } else if (templateJobId != null) {
      final Map<JobId, Job> jobs = client.jobs(templateJobId).get();
      if (jobs.size() == 0) {
        out.printf("Unknown job: %s%n", templateJobId);
        return 1;
      } else if (jobs.size() > 1) {
        out.printf("Ambiguous job reference: %s%n", templateJobId);
        return 1;
      }
      final Job template = Iterables.getOnlyElement(jobs.values());
      builder = template.toBuilder();
      if (id == null) {
        throw new IllegalArgumentException("Please specify new job name and version");
      }
    } else {
      if (id == null || imageIdentifier == null) {
        throw new IllegalArgumentException(
            "Please specify a file, or a template, or a job name, version and container image");
      }
      builder = Job.newBuilder();
    }


    // Merge job configuration options from command line arguments

    if (id != null) {
      final String[] parts = id.split(":");
      switch (parts.length) {
        case 3:
          builder.setHash(parts[2]);
          // fall through
        case 2:
          builder.setVersion(parts[1]);
          // fall through
        case 1:
          builder.setName(parts[0]);
          break;
        default:
          throw new IllegalArgumentException("Invalid Job id: " + id);
      }
    }

    if (imageIdentifier != null) {
      builder.setImage(imageIdentifier);
    }

    final List<String> command = options.getList(argsArg.getDest());
    if (command != null && !command.isEmpty()) {
      builder.setCommand(command);
    }

    final List<List<String>> envList = options.getList(envArg.getDest());
    if (!envList.isEmpty()) {
      final ImmutableMap.Builder<String, String> env = ImmutableMap.builder();
      env.putAll(builder.getEnv());
      for (final List<String> group : envList) {
        for (final String s : group) {
          final String[] parts = s.split("=", 2);
          if (parts.length != 2) {
            throw new IllegalArgumentException("Bad environment variable: " + s);
          }
          env.put(parts[0], parts[1]);
        }
      }
      builder.setEnv(env.build());
    }

    // Parse port mappings
    final List<String> portSpecs = options.getList(portArg.getDest());
    final Map<String, PortMapping> explicitPorts = Maps.newHashMap();
    final Pattern portPattern = compile("(?<n>[_\\-\\w]+)=(?<i>\\d+)(:(?<e>\\d+))?(/(?<p>\\w+))?");
    for (final String spec : portSpecs) {
      final Matcher matcher = portPattern.matcher(spec);
      if (!matcher.matches()) {
        throw new IllegalArgumentException("Bad port mapping: " + spec);
      }

      final String portName = matcher.group("n");
      final int internal = Integer.parseInt(matcher.group("i"));
      final Integer external = nullOrInteger(matcher.group("e"));
      final String protocol = fromNullable(matcher.group("p")).or(TCP);

      if (explicitPorts.containsKey(portName)) {
        throw new IllegalArgumentException("Duplicate port mapping: " + portName);
      }

      explicitPorts.put(portName, PortMapping.of(internal, external, protocol));
    }

    // Merge port mappings
    final Map<String, PortMapping> ports = Maps.newHashMap();
    ports.putAll(builder.getPorts());
    ports.putAll(explicitPorts);
    builder.setPorts(ports);

    // Parse service registrations
    final Map<ServiceEndpoint, ServicePorts> explicitRegistration = Maps.newHashMap();
    final Pattern registrationPattern =
        compile("(?<srv>[a-zA-Z][_\\-\\w]+)(?:/(?<prot>\\w+))?(?:=(?<port>[_\\-\\w]+))?");
    final List<List<String>> registrationSpecLists = options.getList(registrationArg.getDest());
    for (List<String> registrationSpecList : registrationSpecLists) {
      for (final String spec : registrationSpecList) {
        final Matcher matcher = registrationPattern.matcher(spec);
        if (!matcher.matches()) {
          throw new IllegalArgumentException("Bad registration: " + spec);
        }

        final String service = matcher.group("srv");
        final String proto = fromNullable(matcher.group("prot")).or(HTTP);
        final String optionalPort = matcher.group("port");
        final String port;

        if (ports.size() == 0) {
          throw new IllegalArgumentException("Need port mappings for service registration.");
        }

        if (optionalPort == null) {
          if (ports.size() != 1) {
            throw new IllegalArgumentException(
                "Need exactly one port mapping for implicit service registration");
          }
          port = Iterables.getLast(ports.keySet());
        } else {
          port = optionalPort;
        }

        explicitRegistration.put(ServiceEndpoint.of(service, proto), ServicePorts.of(port));
      }
    }

    // Merge service registrations
    final Map<ServiceEndpoint, ServicePorts> registration = Maps.newHashMap();
    registration.putAll(builder.getRegistration());
    registration.putAll(explicitRegistration);
    builder.setRegistration(registration);

    // Parse volumes
    final List<String> volumeSpecs = options.getList(volumeArg.getDest());
    for (final String spec : volumeSpecs) {
      final String[] parts = spec.split(":", 2);
      switch (parts.length) {
        // Data volume
        case 1:
          builder.addVolume(parts[0]);
          break;
        // Bind mount
        case 2:
          final String path = parts[1];
          final String source = parts[0];
          builder.addVolume(path, source);
          break;
        default:
          throw new IllegalArgumentException("Invalid volume: " + spec);
      }
    }

    final Job job = builder.build();

    final Collection<String> errors = JOB_VALIDATOR.validate(job);
    if (!errors.isEmpty()) {
      // TODO: nicer output
      throw new IllegalArgumentException("Bad job definition: " + errors);
    }

    if (!quiet && !json) {
      out.println("Creating job: " + job.toJsonString());
    }

    final CreateJobResponse status = client.createJob(job).get();
    if (status.getStatus() == CreateJobResponse.Status.OK) {
      if (!quiet && !json) {
        out.println("Done.");
      }
      if (json) {
        out.println("\"" + job.getId() + "\"");
      } else {
        out.println(job.getId());
      }
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

