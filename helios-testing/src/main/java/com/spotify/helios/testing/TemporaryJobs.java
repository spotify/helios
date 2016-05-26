/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.testing;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import com.typesafe.config.ConfigValueType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Integer.toHexString;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TemporaryJobs implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(TemporaryJobs.class);

  private static final String HELIOS_TESTING_PROFILE = "helios.testing.profile";
  private static final String HELIOS_TESTING_PROFILES = "helios.testing.profiles.";
  private static final Prober DEFAULT_PROBER = new DefaultProber();
  private static final long DEFAULT_DEPLOY_TIMEOUT_MILLIS = MINUTES.toMillis(10);

  private final HeliosDeployment heliosDeployment;
  private final HeliosClient client;
  private final Prober prober;
  private final String defaultHostFilter;
  private final Config config;
  private final Map<String, String> env;
  private final List<TemporaryJob> jobs = Lists.newCopyOnWriteArrayList();
  private final Deployer deployer;
  private final Undeployer undeployer;
  private final String jobPrefix;

  private static volatile HeliosDeployment soloDeployment;

  TemporaryJobs(final Builder builder, final Config config) {
    if (builder.heliosDeployment != null) {
      this.heliosDeployment = builder.heliosDeployment;
    } else {
      this.heliosDeployment = getOrCreateHeliosSoloDeployment();
    }
    client = checkNotNull(heliosDeployment.client(), "client");
    this.prober = checkNotNull(builder.prober, "prober");
    this.defaultHostFilter = checkNotNull(builder.hostFilter, "hostFilter");
    this.env = checkNotNull(builder.env, "env");
    this.jobPrefix = checkNotNull(builder.jobPrefix, "jobPrefix");

    checkArgument(builder.deployTimeoutMillis >= 0, "deployTimeoutMillis");

    this.undeployer = new DefaultUndeployer(client);
    this.deployer = fromNullable(builder.deployer).or(
        new DefaultDeployer(client, jobs, builder.hostPickingStrategy,
            builder.jobDeployedMessageFormat, builder.deployTimeoutMillis, undeployer));

    // Load in the prefix so it can be used in the config
    final Config configWithPrefix = ConfigFactory.empty()
        .withValue("prefix", ConfigValueFactory.fromAnyRef(jobPrefix));

    this.config = config.withFallback(configWithPrefix).resolve();
  }

  private static synchronized HeliosDeployment getOrCreateHeliosSoloDeployment() {
    if (soloDeployment == null) {
      // TODO (dxia) remove checkForNewImages(). Set here to prevent using
      // spotify/helios-solo:latest from docker hub
      soloDeployment = HeliosSoloDeployment.fromEnv()
        .checkForNewImages(false)
        .build();
    }

    return soloDeployment;
  }

  public TemporaryJobBuilder job() {
    return this.job(Job.newBuilder());
  }

  public TemporaryJobBuilder jobWithConfig(final String configFile) throws IOException {
    checkNotNull(configFile);

    final Path configPath = Paths.get(configFile);
    final File file = configPath.toFile();

    if (!file.exists() || !file.isFile() || !file.canRead()) {
      throw new IllegalArgumentException("Cannot read file " + file);
    }

    final byte[] bytes = Files.readAllBytes(configPath);
    final String config = new String(bytes, UTF_8);
    final Job job = Json.read(config, Job.class);

    return this.job(job.toBuilder());
  }

  private TemporaryJobBuilder job(final Job.Builder jobBuilder) {
    final TemporaryJobBuilder builder = new TemporaryJobBuilder(
        deployer, jobPrefix, prober, env, jobBuilder);

    if (config.hasPath("env")) {
      final Config env = config.getConfig("env");

      for (final Entry<String, ConfigValue> entry : env.entrySet()) {
        builder.env(entry.getKey(), entry.getValue().unwrapped());
      }
    }

    if (config.hasPath("version")) {
      builder.version(config.getString("version"));
    }
    if (config.hasPath("image")) {
      builder.image(config.getString("image"));
    }
    if (config.hasPath("command")) {
      builder.command(getListByKey("command", config));
    }
    if (config.hasPath("host")) {
      builder.host(config.getString("host"));
    }
    if (config.hasPath("deploy")) {
      builder.deploy(getListByKey("deploy", config));
    }
    if (config.hasPath("imageInfoFile")) {
      builder.imageFromInfoFile(config.getString("imageInfoFile"));
    }
    if (config.hasPath("registrationDomain")) {
      builder.registrationDomain(config.getString("registrationDomain"));
    }
    // port and expires intentionally left out -- since expires is a specific point in time, I
    // cannot imagine a config-file use for it, additionally for ports, I'm thinking that port
    // allocations are not likely to be common -- but PR's welcome if I'm wrong. - drewc@spotify.com
    builder.hostFilter(defaultHostFilter);
    return builder;
  }

  private static List<String> getListByKey(final String key, final Config config) {
    final ConfigList endpointList = config.getList(key);
    final List<String> stringList = Lists.newArrayList();
    for (final ConfigValue v : endpointList) {
      if (v.valueType() != ConfigValueType.STRING) {
        throw new RuntimeException("Item in " + key + " list [" + v + "] is not a string");
      }
      stringList.add((String) v.unwrapped());
    }
    return stringList;
  }

  /**
   * Creates a new instance of TemporaryJobs. Will attempt start a helios-solo container which
   * has a master, ZooKeeper, and one agent.
   *
   * @return an instance of TemporaryJobs
   * @see <a href=
   * "https://github.com/spotify/helios/blob/master/docs/testing_framework.md#configuration-by-file"
   * >Helios Testing Framework - Configuration By File</a>
   */
  public static TemporaryJobs create() {
    return builder().build();
  }

  public static TemporaryJobs create(final HeliosClient client) {
    return builder().heliosDeployment(
        ExistingHeliosDeployment.newBuilder().heliosClient(client).build())
        .build();
  }

  public static TemporaryJobs create(final String domain) {
    return builder().heliosDeployment(
        ExistingHeliosDeployment.newBuilder().domain(domain).build())
        .build();
  }

  public static TemporaryJobs createFromProfile(final String profile) {
    return builder(profile).build();
  }

  String jobPrefix() {
    return jobPrefix;
  }

  public HeliosClient client() {
    return client;
  }

  List<TemporaryJob> jobs() {
    return jobs;
  }

  Deployer deployer() {
    return deployer;
  }

  public List<AssertionError> undeploy(final Job job, final List<String> hosts) {
    return undeployer.undeploy(job, hosts);
  }

  public static Builder builder() {
    return builder((String) null);
  }

  public static Builder builder(final String profile) {
    return builder(profile, System.getenv());
  }

  static Builder builder(final Map<String, String> env) {
    return builder(null, env);
  }

  static Builder builder(final String profile, final Map<String, String> env) {
    return new Builder(profile, HeliosConfig.loadConfig("helios-testing"), env);
  }

  @Override
  public void close() {
    final List<AssertionError> errors = new ArrayList<>();

    for (final TemporaryJob job : jobs) {
      // Undeploying a job doesn't guarantee the container will be stopped immediately.
      // Luckily TaskRunner tells docker to wait 120 seconds after stopping to kill the container.
      // So containers that don't immediately stop **should** only stay around for at most 120
      // seconds.
      errors.addAll(undeploy(job.job(), job.hosts()));
    }

    for (final AssertionError error : errors) {
      log.error(error.getMessage());
    }
  }

  public static class Builder {

    private final Map<String, String> env;
    private final Config config;
    private Prober prober = DEFAULT_PROBER;
    private Deployer deployer;
    private String hostFilter;
    private HeliosDeployment heliosDeployment;
    // TODO (dxia) This random string logic is duplicated in JobPrefixFile
    private String jobPrefix = String.format(
        "tmp-%s-%s", new SimpleDateFormat("yyyyMMdd").format(new Date()),
        toHexString(ThreadLocalRandom.current().nextInt()));
    private String jobDeployedMessageFormat;
    private HostPickingStrategy hostPickingStrategy = HostPickingStrategies.randomOneHost();
    private long deployTimeoutMillis = DEFAULT_DEPLOY_TIMEOUT_MILLIS;

    Builder(final String profile, final Config rootConfig, final Map<String, String> env) {
      this.env = env;

      if (profile == null) {
        this.config = HeliosConfig.getDefaultProfile(
            HELIOS_TESTING_PROFILE, HELIOS_TESTING_PROFILES, rootConfig);
      } else {
        this.config = HeliosConfig.getProfile(HELIOS_TESTING_PROFILES, profile, rootConfig);
      }

      ExistingHeliosDeployment existingHeliosDeployment = null;

      if (this.config.hasPath("jobDeployedMessageFormat")) {
        jobDeployedMessageFormat(this.config.getString("jobDeployedMessageFormat"));
      }
      if (this.config.hasPath("hostFilter")) {
        hostFilter(this.config.getString("hostFilter"));
      }

      if (this.config.hasPath("domain")) {
        existingHeliosDeployment = ExistingHeliosDeployment.newBuilder()
            .domain(this.config.getString("domain"))
            .build();
      } else if (this.config.hasPath("endpoints")) {
        existingHeliosDeployment = ExistingHeliosDeployment.newBuilder()
            .endpointStrings(getListByKey("endpoints", config))
            .build();
      }

      if (this.config.hasPath("hostPickingStrategy")) {
        processHostPickingStrategy();
      }
      if (this.config.hasPath("deployTimeoutMillis")) {
        deployTimeoutMillis(this.config.getLong("deployTimeoutMillis"));
      }

      heliosDeployment = existingHeliosDeployment;
    }

    private void processHostPickingStrategy() {
      final String value = this.config.getString("hostPickingStrategy");
      if ("random".equals(value)) {
        hostPickingStrategy(HostPickingStrategies.random());

      } else if ("onerandom".equals(value)) {
        hostPickingStrategy(HostPickingStrategies.randomOneHost());

      } else if ("deterministic".equals(value)) {
        verifyHasStrategyKey(value);
        hostPickingStrategy(HostPickingStrategies.deterministic(
            this.config.getString("hostPickingStrategyKey")));

      } else if ("onedeterministic".equals(value)) {
        verifyHasStrategyKey(value);
        hostPickingStrategy(HostPickingStrategies.deterministicOneHost(
            this.config.getString("hostPickingStrategyKey")));

      } else {
        throw new RuntimeException("The hostPickingStrategy " + value + " is not valid. "
          + "Valid values are [random, onerandom, deterministic, onedeterministic] and the "
          + "deterministic variants require a string value hostPickingStrategyKey to be set "
          + "which is used to seed the random number generator, so can be any string.");
      }
    }

    private void verifyHasStrategyKey(final String value) {
      if (!this.config.hasPath("hostPickingStrategyKey")) {
        throw new RuntimeException("host picking strategy [" + value + "] selected but no "
            + "value for hostPickingStrategyKey which is used to seed the random number generator");
      }
    }

    public Builder heliosDeployment(final HeliosDeployment heliosDeployment) {
      this.heliosDeployment = heliosDeployment;
      return this;
    }

    public Builder hostPickingStrategy(final HostPickingStrategy strategy) {
      this.hostPickingStrategy = strategy;
      return this;
    }

    public Builder jobDeployedMessageFormat(final String jobLinkFormat) {
      this.jobDeployedMessageFormat = jobLinkFormat;
      return this;
    }

    public Builder prober(final Prober prober) {
      this.prober = prober;
      return this;
    }

    Builder deployer(final Deployer deployer) {
      this.deployer = deployer;
      return this;
    }

    public Builder hostFilter(final String hostFilter) {
      this.hostFilter = hostFilter;
      return this;
    }

    public Builder jobPrefix(final String jobPrefix) {
      this.jobPrefix = jobPrefix;
      return this;
    }

    public Builder deployTimeoutMillis(final long timeout) {
      this.deployTimeoutMillis = timeout;
      return this;
    }

    public TemporaryJobs build() {
      return new TemporaryJobs(this, config);
    }
  }
}
