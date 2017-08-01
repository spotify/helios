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

package com.spotify.helios.cli.command;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.helios.common.descriptors.Job.EMPTY_TOKEN;
import static com.spotify.helios.common.descriptors.ServiceEndpoint.HTTP;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.regex.Pattern.compile;
import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.fileType;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.JobValidator;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.ExecHealthCheck;
import com.spotify.helios.common.descriptors.HttpHealthCheck;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.descriptors.TcpHealthCheck;
import com.spotify.helios.common.protocol.CreateJobResponse;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.joda.time.DateTime;

public class JobCreateCommand extends ControlCommand {

  private static final JobValidator JOB_VALIDATOR = new JobValidator(false);

  /**
   * If any of the keys of this map are set as environment variables (i.e. an environment variable
   * of GIT_COMMIT_ID=abcdef is set when this command is run), then an entry will be added to the
   * job's metadata map with with key=this-maps-value, value=environment variable value.
   *
   * @see #defaultMetadata()
   */
  public static final Map<String, String> DEFAULT_METADATA_ENVVARS = ImmutableMap.of(
      // GIT_COMMIT is set by the Git plugin in Jenkins, so for jobs created in
      // Jenkins this will automatically set GIT_COMMIT = the sha1 of the
      // working tree
      "GIT_COMMIT", "GIT_COMMIT"
  );

  // allow the retrieval of environment variables to be swapped out with a different Supplier for
  // testing purposes
  private static final Supplier<Map<String, String>> DEFAULT_ENV_VAR_SUPPLIER =
      new Supplier<Map<String, String>>() {
        @Override
        public Map<String, String> get() {
          return System.getenv();
        }
      };

  private final Argument fileArg;
  private final Argument templateArg;
  private final Argument quietArg;
  private final Argument idArg;
  private final Argument imageArg;
  private final Argument hostnameArg;
  private final Argument tokenArg;
  private final Argument envArg;
  private final Argument argsArg;
  private final Argument portArg;
  private final Argument registrationArg;
  private final Argument registrationDomainArg;
  private final Argument gracePeriodArg;
  private final Argument volumeArg;
  private final Argument expiresArg;
  private final Argument healthCheckExecArg;
  private final Argument healthCheckHttpArg;
  private final Argument healthCheckTcpArg;
  private final Argument securityOptArg;
  private final Argument networkModeArg;
  private final Argument metadataArg;
  private final Argument addCapabilityArg;
  private final Argument dropCapabilityArg;
  private final Argument labelsArg;
  private final Supplier<Map<String, String>> envVarSupplier;

  public JobCreateCommand(final Subparser parser) {
    this(parser, DEFAULT_ENV_VAR_SUPPLIER);
  }

  /**
   * Allows the supplier of environment variables to be swapped out for testing, for example to
   * avoid unexpected environment variables being present during testing.
   */
  @VisibleForTesting
  protected JobCreateCommand(final Subparser parser, Supplier<Map<String, String>> envVarSupplier) {
    super(parser);

    parser.help("create a job");

    fileArg = parser.addArgument("-f", "--file")
        .type(fileType().acceptSystemIn())
        .help("Job configuration file. Options specified on the command line will be merged with"
              + " the contents of this file. Cannot be used together with -t/--template.");

    templateArg = parser.addArgument("-t", "--template")
        .help("Template job id. The new job will be based on this job. Cannot be used together "
              + "with -f/--file.");

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print job id");

    idArg = parser.addArgument("id")
        .nargs("?")
        .help("Job name:version[:hash]");

    imageArg = parser.addArgument("image")
        .nargs("?")
        .help("Container image");

    hostnameArg = parser.addArgument("--hostname")
        .nargs("?")
        .help("Container hostname");

    tokenArg = parser.addArgument("--token")
        .nargs("?")
        .setDefault(EMPTY_TOKEN)
        .help("Insecure access token meant to prevent accidental changes to your job "
              + "(e.g. undeploys).");

    envArg = parser.addArgument("--env")
        .action(append())
        .setDefault(new ArrayList<String>())
        .help("Environment variables");

    metadataArg = parser.addArgument("-m", "--metadata")
        .action(append())
        .setDefault(new ArrayList<String>())
        .help("Metadata (key-value pairs) to associate with job");

    portArg = parser.addArgument("-p", "--port")
        .action(append())
        .setDefault(new ArrayList<String>())
        .help("Port mapping of the form "
              + "<name>=[<IP address>:]<internal port>[:<external port>][/<protocol>].\n\n"
              + "The IP address defaults to \"0.0.0.0\". Protocol defaults to \"tcp\".\n\n"
              + "Specify a name and an internal port (e.g. \"http=8080\") for "
              + "dynamic port mapping. Specify name, internal port, and external port "
              + "(e.g. \"http=8080:80\") for static port mapping.\n\n"
              + "For example, foo=4711 will map the internal port 4711 of the "
              + "container to an arbitrary external port on the host exposed on 0.0.0.0. "
              + "Specifying foo=127.0.0.1:4711:80 will map internal port 4711 of the container to "
              + "port 80 on the host and exposed only on loopback.\n\n"
              + "For UDP, add /udp. E.g. quic=80/udp or dns=53:53/udp. The name can be "
              + "used when specifying service registration using -r/--register.");

    registrationArg = parser.addArgument("-r", "--register")
        .action(append())
        .setDefault(new ArrayList<String>())
        .help("Service discovery registration. Specify a service name, the port name and a "
              + "protocol on the format service/protocol=port. E.g. -r website/tcp=http will "
              + "register the port named http with the protocol tcp. Protocol is optional and "
              + "default is tcp. If there is only one port mapping, this will be used by "
              + "default and it will be enough to specify only the service name, e.g. "
              + "-r wordpress.");

    registrationDomainArg = parser.addArgument("--registration-domain")
        .setDefault("")
        .help("If set, overrides the default domain in which discovery serviceregistration "
              + "occurs. What is allowed here will vary based upon the discovery service plugin "
              + "used.");

    gracePeriodArg = parser.addArgument("--grace-period")
        .type(Integer.class)
        .setDefault((Object) null)
        .help("if --grace-period is specified, Helios will unregister from service discovery and "
              + "wait the specified number of seconds before undeploying, default 0 seconds");

    volumeArg = parser.addArgument("--volume")
        .action(append())
        .setDefault(new ArrayList<String>())
        .help("Container volumes. Specify either a single path to create a data volume, "
              + "or a source path and a container path to mount a file or directory from the host. "
              + "The container path can be suffixed with \"rw\" or \"ro\" to create a read-write "
              + "or read-only volume, respectively. Format: [container-path]:[host-path]:[rw|ro].");

    argsArg = parser.addArgument("args")
        .nargs("*")
        .help("Command line arguments");

    expiresArg = parser.addArgument("-e", "--expires")
        .help("An ISO-8601 string representing the date/time when this job should expire. The "
              + "job will be undeployed from all hosts and removed at this time. E.g. "
              + "2014-06-01T12:00:00Z");

    healthCheckExecArg = parser.addArgument("--exec-check")
        .help("Run `docker exec` health check with the provided command. The service will not be "
              + "registered in service discovery until the command executes successfully in the "
              + "container, i.e. with exit code 0. E.g. --exec-check ping google.com");

    healthCheckHttpArg = parser.addArgument("--http-check")
        .help("Run HTTP health check against the provided port name and path. The service will "
              + "not be registered in service discovery until the container passes the HTTP health "
              + "check. Format: [port name]:[path].");

    healthCheckTcpArg = parser.addArgument("--tcp-check")
        .help("Run TCP health check against the provided port name. The service will not be "
              + "registered in service discovery until the container passes the TCP health check.");

    securityOptArg = parser.addArgument("--security-opt")
        .action(append())
        .setDefault(Lists.newArrayList())
        .help("Run the Docker container with a security option. "
              + "See https://docs.docker.com/reference/run/#security-configuration.");

    networkModeArg = parser.addArgument("--network-mode")
        .help("Sets the networking mode for the container. Supported values are: bridge, host, and "
              + "container:<name|id>. Docker defaults to bridge.");

    addCapabilityArg = parser.addArgument("--add-capability")
        .action(append())
        .setDefault(new ArrayList<String>())
        .help("The Linux capabilities this Helios job adds to its Docker container. "
              + "Defaults to nothing.");

    dropCapabilityArg = parser.addArgument("--drop-capability")
        .action(append())
        .setDefault(new ArrayList<String>())
        .help("The Linux capabilities this Helios job drops from its Docker container. "
              + "Defaults to nothing.");

    labelsArg = parser.addArgument("--labels")
        .action(append())
        .setDefault(new ArrayList<String>())
        .help("Labels (key-value pairs) to apply onto the container. "
              + "Defaults to nothing.");

    this.envVarSupplier = envVarSupplier;
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException {

    final boolean quiet = options.getBoolean(quietArg.getDest());

    final Job.Builder builder;

    final String id = options.getString(idArg.getDest());
    final String imageIdentifier = options.getString(imageArg.getDest());

    // Read job configuration from file

    // TODO (dano): look for e.g. Heliosfile in cwd by default?

    final String templateJobId = options.getString(templateArg.getDest());
    final File file = options.get(fileArg.getDest());

    if (file != null && templateJobId != null) {
      throw new IllegalArgumentException("Please use only one of -t/--template and -f/--file");
    }

    if (file != null) {
      if (!file.exists() || !file.isFile() || !file.canRead()) {
        throw new IllegalArgumentException("Cannot read file " + file);
      }
      final byte[] bytes = Files.readAllBytes(file.toPath());
      final String config = new String(bytes, UTF_8);
      final Job job = Json.read(config, Job.class);
      builder = job.toBuilder();
    } else if (templateJobId != null) {
      final Map<JobId, Job> jobs = client.jobs(templateJobId).get();
      if (jobs.size() == 0) {
        if (!json) {
          out.printf("Unknown job: %s%n", templateJobId);
        } else {
          final CreateJobResponse createJobResponse =
              new CreateJobResponse(CreateJobResponse.Status.UNKNOWN_JOB, null, null);
          out.print(createJobResponse.toJsonString());
        }
        return 1;
      } else if (jobs.size() > 1) {
        if (!json) {
          out.printf("Ambiguous job reference: %s%n", templateJobId);
        } else {
          final CreateJobResponse createJobResponse =
              new CreateJobResponse(CreateJobResponse.Status.AMBIGUOUS_JOB_REFERENCE, null, null);
          out.print(createJobResponse.toJsonString());
        }
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

    final String hostname = options.getString(hostnameArg.getDest());
    if (!isNullOrEmpty(hostname)) {
      builder.setHostname(hostname);
    }

    final List<String> command = options.getList(argsArg.getDest());
    if (command != null && !command.isEmpty()) {
      builder.setCommand(command);
    }

    final List<String> envList = options.getList(envArg.getDest());
    // TODO (mbrown): does this mean that env config is only added when there is a CLI flag too?
    if (!envList.isEmpty()) {
      final Map<String, String> env = Maps.newHashMap();
      // Add environmental variables from helios job configuration file
      env.putAll(builder.getEnv());
      // Add environmental variables passed in via CLI
      // Overwrite any redundant keys to make CLI args take precedence
      env.putAll(parseListOfPairs(envList, "environment variable"));

      builder.setEnv(env);
    }

    final Map<String, String> metadata = Maps.newHashMap();
    metadata.putAll(defaultMetadata());
    final List<String> metadataList = options.getList(metadataArg.getDest());
    if (!metadataList.isEmpty()) {
      // TODO (mbrown): values from job conf file (which maybe involves dereferencing env vars?)
      metadata.putAll(parseListOfPairs(metadataList, "metadata"));
    }
    builder.setMetadata(metadata);

    // Parse port mappings
    final List<String> portSpecs = options.getList(portArg.getDest());
    final Map<String, PortMapping> explicitPorts = PortMappingParser.parsePortMappings(portSpecs);

    // Merge port mappings
    final Map<String, PortMapping> ports = Maps.newHashMap();
    ports.putAll(builder.getPorts());
    ports.putAll(explicitPorts);
    builder.setPorts(ports);

    // Parse service registrations
    final Map<ServiceEndpoint, ServicePorts> explicitRegistration = Maps.newHashMap();
    final Pattern registrationPattern =
        compile("(?<srv>[a-zA-Z][_\\-\\w]+)(?:/(?<prot>\\w+))?(?:=(?<port>[_\\-\\w]+))?");
    final List<String> registrationSpecs = options.getList(registrationArg.getDest());
    for (final String spec : registrationSpecs) {
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

    final String registrationDomain = options.getString(registrationDomainArg.getDest());
    if (!isNullOrEmpty(registrationDomain)) {
      builder.setRegistrationDomain(registrationDomain);
    }

    // Merge service registrations
    final Map<ServiceEndpoint, ServicePorts> registration = Maps.newHashMap();
    registration.putAll(builder.getRegistration());
    registration.putAll(explicitRegistration);
    builder.setRegistration(registration);

    // Get grace period interval
    final Integer gracePeriod = options.getInt(gracePeriodArg.getDest());
    if (gracePeriod != null) {
      builder.setGracePeriod(gracePeriod);
    }

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

    // Parse expires timestamp
    final String expires = options.getString(expiresArg.getDest());
    if (expires != null) {
      // Use DateTime to parse the ISO-8601 string
      builder.setExpires(new DateTime(expires).toDate());
    }

    // Parse health check
    final String execString = options.getString(healthCheckExecArg.getDest());
    final List<String> execHealthCheck =
        (execString == null) ? null : Arrays.asList(execString.split(" "));
    final String httpHealthCheck = options.getString(healthCheckHttpArg.getDest());
    final String tcpHealthCheck = options.getString(healthCheckTcpArg.getDest());

    int numberOfHealthChecks = 0;
    for (final String c : asList(httpHealthCheck, tcpHealthCheck)) {
      if (!isNullOrEmpty(c)) {
        numberOfHealthChecks++;
      }
    }
    if (execHealthCheck != null && !execHealthCheck.isEmpty()) {
      numberOfHealthChecks++;
    }

    if (numberOfHealthChecks > 1) {
      throw new IllegalArgumentException("Only one health check may be specified.");
    }

    if (execHealthCheck != null && !execHealthCheck.isEmpty()) {
      builder.setHealthCheck(ExecHealthCheck.of(execHealthCheck));
    } else if (!isNullOrEmpty(httpHealthCheck)) {
      final String[] parts = httpHealthCheck.split(":", 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid HTTP health check: " + httpHealthCheck);
      }

      builder.setHealthCheck(HttpHealthCheck.of(parts[0], parts[1]));
    } else if (!isNullOrEmpty(tcpHealthCheck)) {
      builder.setHealthCheck(TcpHealthCheck.of(tcpHealthCheck));
    }

    final List<String> securityOpt = options.getList(securityOptArg.getDest());
    if (securityOpt != null && !securityOpt.isEmpty()) {
      builder.setSecurityOpt(securityOpt);
    }

    final String networkMode = options.getString(networkModeArg.getDest());
    if (!isNullOrEmpty(networkMode)) {
      builder.setNetworkMode(networkMode);
    }

    final String token = options.getString(tokenArg.getDest());
    if (!isNullOrEmpty(token)) {
      builder.setToken(token);
    }

    final List<String> addCaps = options.getList(addCapabilityArg.getDest());
    if (addCaps != null && !addCaps.isEmpty()) {
      builder.setAddCapabilities(addCaps);
    }

    final List<String> dropCaps = options.getList(dropCapabilityArg.getDest());
    if (dropCaps != null && !dropCaps.isEmpty()) {
      builder.setDropCapabilities(dropCaps);
    }

    final List<String> labelsList = options.getList(labelsArg.getDest());
    if (!labelsList.isEmpty()) {
      final Map<String, String> labels = Maps.newHashMap();
      labels.putAll(parseListOfPairs(labelsList, "labels"));
      builder.setLabels(labels);
    }

    // We build without a hash here because we want the hash to be calculated server-side.
    // This allows different CLI versions to be cross-compatible with different master versions
    // that have either more or fewer job parameters.
    final Job job = builder.buildWithoutHash();

    final Collection<String> errors = JOB_VALIDATOR.validate(job);
    if (!errors.isEmpty()) {
      if (!json) {
        for (final String error : errors) {
          out.println(error);
        }
      } else {
        final CreateJobResponse createJobResponse = new CreateJobResponse(
            CreateJobResponse.Status.INVALID_JOB_DEFINITION, ImmutableList.copyOf(errors),
            job.getId().toString());
        out.println(createJobResponse.toJsonString());
      }

      return 1;
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
        out.println(status.toJsonString());
      } else {
        out.println(status.getId());
      }
      return 0;
    } else {
      if (!quiet && !json) {
        out.println("Failed: " + status);
      } else if (json) {
        out.println(status.toJsonString());
      }
      return 1;
    }
  }

  /**
   * Metadata to associate with jobs by default. Currently sets some metadata based upon environment
   * variables set when the CLI command is run.
   */
  private Map<String, String> defaultMetadata() {

    final Builder<String, String> builder = ImmutableMap.builder();

    final Map<String, String> envVars = envVarSupplier.get();

    for (final Map.Entry<String, String> entry : DEFAULT_METADATA_ENVVARS.entrySet()) {
      final String envKey = entry.getKey();
      final String metadataKey = entry.getValue();

      final String envValue = envVars.get(envKey);
      if (envValue != null) {
        builder.put(metadataKey, envValue);
      }
    }

    return builder.build();
  }

  private static Map<String, String> parseListOfPairs(final List<String> list,
                                                      final String fieldName) {
    return parseListOfPairs(list, fieldName, "=");
  }

  private static Map<String, String> parseListOfPairs(final List<String> list,
                                                      final String fieldName,
                                                      final String delimiter) {
    final Map<String, String> pairs = new HashMap<>();
    for (final String s : list) {
      final String[] parts = s.split(delimiter, 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException(
            format("Bad format for %s: '%s', expecting %s-delimited pairs",
                fieldName,
                s,
                delimiter));
      }
      pairs.put(parts[0], parts[1]);
    }
    return pairs;
  }

  private Integer nullOrInteger(final String str) {
    return str == null ? null : Integer.valueOf(str);
  }
}

