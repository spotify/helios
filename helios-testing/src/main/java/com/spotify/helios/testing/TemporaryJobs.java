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

package com.spotify.helios.testing;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import com.typesafe.config.ConfigValueType;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.spotify.helios.testing.Jobs.undeploy;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TemporaryJobs implements TestRule {
  private static final Logger log = LoggerFactory.getLogger(TemporaryJobs.class);

  static final String HELIOS_TESTING_PROFILE = "helios.testing.profile";
  private static final String HELIOS_TESTING_PROFILES = "helios.testing.profiles.";
  private static final String DEFAULT_USER = System.getProperty("user.name");
  private static final Prober DEFAULT_PROBER = new DefaultProber();
  private static final String DEFAULT_LOCAL_HOST_FILTER = ".+";
  private static final String DEFAULT_PREFIX_DIRECTORY = "/tmp/helios-temp-jobs";
  private static final long JOB_HEALTH_CHECK_INTERVAL_MILLIS = SECONDS.toMillis(5);

  private final HeliosClient client;
  private final Prober prober;
  private final String defaultHostFilter;
  private final JobPrefixFile jobPrefixFile;
  private final Config config;
  private final List<TemporaryJob> jobs = Lists.newCopyOnWriteArrayList();
  private final Deployer deployer;
  private final String jobDeployedMessageFormat;

  private final ExecutorService executor = MoreExecutors.getExitingExecutorService(
      (ThreadPoolExecutor) Executors.newFixedThreadPool(
          1, new ThreadFactoryBuilder()
              .setNameFormat("helios-test-runner-%d")
              .setDaemon(true)
              .build()),
      0, SECONDS);

  TemporaryJobs(final Builder builder, final Config config) {
    this.client = checkNotNull(builder.client, "client");
    this.prober = checkNotNull(builder.prober, "prober");
    this.defaultHostFilter = checkNotNull(builder.hostFilter, "hostFilter");
    this.deployer = Optional.fromNullable(builder.deployer).or(
        new DefaultDeployer(client, jobs, builder.hostPickingStrategy));
    final Path prefixDirectory = Paths.get(Optional.fromNullable(builder.prefixDirectory)
        .or(DEFAULT_PREFIX_DIRECTORY));

    try {
      removeOldJobs(prefixDirectory);
      this.jobPrefixFile = JobPrefixFile.create(prefixDirectory);
    } catch (IOException | ExecutionException | InterruptedException e) {
      throw Throwables.propagate(e);
    }

    // Load in the prefix so it can be used in the config
    final Config configWithPrefix = ConfigFactory.empty()
        .withValue("prefix", ConfigValueFactory.fromAnyRef(prefix()));

    this.config = config.withFallback(configWithPrefix).resolve();
    this.jobDeployedMessageFormat = Optional.fromNullable(builder.jobDeployedMessageFormat).or("");
  }

  /**
   * Perform setup. This is normally called by JUnit when TemporaryJobs is used with @Rule.
   * If @Rule cannot be used, call this method before calling {@link #job()}.
   *
   * Note: When not being used as a @Rule, jobs will not be monitored during test runs.
   */
  public void before() {
    deployer.readyToDeploy();
  }

  /**
   * Perform teardown. This is normally called by JUnit when TemporaryJobs is used with @Rule.
   * If @Rule cannot be used, call this method after running tests.
   */
  public void after() {
    // Stop the test runner thread
    executor.shutdownNow();
    try {
      final boolean terminated = executor.awaitTermination(30, SECONDS);
      if (!terminated) {
        log.warn("Failed to stop test runner thread");
      }
    } catch (InterruptedException ignore) {
    }

    final List<AssertionError> errors = newArrayList();

    for (TemporaryJob job : jobs) {
      job.undeploy(errors);
    }

    for (AssertionError error : errors) {
      log.error(error.getMessage());
    }

    // Don't delete the prefix file if any errors occurred during undeployment, so that we'll
    // try to undeploy them the next time TemporaryJobs is run.
    if (errors.isEmpty()) {
      jobPrefixFile.delete();
    }
  }

  public TemporaryJobBuilder job() {
    final TemporaryJobBuilder builder = new TemporaryJobBuilder(deployer, jobPrefixFile.prefix(),
                                                                prober, jobDeployedMessageFormat);

    if (config.hasPath("env")) {
      final Config env = config.getConfig("env");

      for (final Entry<String, ConfigValue> entry : env.entrySet()) {
        builder.env(entry.getKey(), entry.getValue().unwrapped());
      }
    }

    if (config.hasPath("name")) {
      builder.name(config.getString("name"));
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
   * Creates a new instance of TemporaryJobs. Will attempt to connect to a helios master according
   * to the following factors, where the order of precedence is top to bottom.
   * <ol>
   * <li>HELIOS_DOMAIN - If set, use a helios master running in this domain.</li>
   * <li>HELIOS_ENDPOINTS - If set, use one of the endpoints, which are specified as a comma
   * separated list.</li>
   * <li>Testing Profile - If a testing profile can be loaded, use either {@code domain} or
   * <tt>endpoints</tt> if present. If both are specified, {@code domain} takes precedence.</li>
   * <li>DOCKER_HOST - If set, assume a helios master is running on this host, so connect to it on
   * port {@code 5801}.</li>
   * <li>Use {@code http://localhost:5801}</li>
   * </ol>
   *
   * @return an instance of TemporaryJobs
   * @see <a href="https://github.com/spotify/helios/blob/master/docs/testing_framework.md#
   * configuration-by-file">Helios Testing Framework - Configuration By File</a>
   */

  public static TemporaryJobs create() {
    // Builder will be initialized with values from a testing profile if one is present. If domain
    // or endpoints is specified in the profile, they will be used to create a helios client.
    final Builder builder = builder();

    // If set, use HELIOS_HOST_FILTER to override value from test profile
    final String heliosHostFilter = System.getenv("HELIOS_HOST_FILTER");
    if (heliosHostFilter != null) {
      builder.hostFilter(heliosHostFilter);
    }

    // If set, use HELIOS_DOMAIN to override client which may have been created from test profile
    final String domain = System.getenv("HELIOS_DOMAIN");
    if (!isNullOrEmpty(domain)) {
      return builder.domain(domain).build();
    }

    // If set, use HELIOS_ENDPOINTS to override client which may have been created from test profile
    final String endpoints = System.getenv("HELIOS_ENDPOINTS");
    if (!isNullOrEmpty(endpoints)) {
      return builder.endpointStrings(Splitter.on(',').splitToList(endpoints)).build();
    }

    // If we get here and client is set in the builder, it was created using a testing profile,
    // and the values were not overridden by environment variables. Let's use it.
    if (builder.client != null) {
      return builder.build();
    }

    // If we get here, we were not able to create a client based on environment variables or a
    // testing profile, so check if DOCKER_HOST is set. If so, try to connect to that host on port
    // 5801, assuming it has a helios master running. If not, attempt to connect to
    // http://localhost:5801 as a last attempt.
    final String dockerHost = System.getenv("DOCKER_HOST");
    if (dockerHost == null) {
      builder.endpoints("http://localhost:5801");
    } else {
      try {
        final URI uri = new URI(dockerHost);
        builder.endpoints("http://" + uri.getHost() + ":5801");
      } catch (URISyntaxException e) {
        throw Throwables.propagate(e);
      }
    }

    // We usually require the caller to specify a host filter, so jobs aren't accidentally deployed
    // to arbitrary hosts. But at this point the master is either running on localhost or the docker
    // host. Either way, this is probably a test machine with one master and one agent both running
    // on the same box, so it is safe to provide a default filter that will deploy anywhere.
    if (heliosHostFilter == null) {
      builder.hostFilter(DEFAULT_LOCAL_HOST_FILTER);
    }

    return builder.build();
  }

  public static TemporaryJobs create(final HeliosClient client) {
    return builder().client(client).build();
  }

  public static TemporaryJobs create(final String domain) {
    return builder().domain(domain).build();
  }

  public static TemporaryJobs createFromProfile(final String profile) {
    return builder(profile).build();
  }

  @Override
  public Statement apply(final Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        before();
        try {
          perform(base);
        } finally {
          after();
        }
      }
    };
  }

  private void perform(final Statement base)
      throws InterruptedException {
    // Run the actual test on a thread
    final Future<Object> future = executor.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        try {
          base.evaluate();
        } catch (Throwable throwable) {
          Throwables.propagateIfPossible(throwable, Exception.class);
          throw Throwables.propagate(throwable);
        }
        return null;
      }
    });

    // Monitor jobs while test is running
    while (!future.isDone()) {
      Thread.sleep(JOB_HEALTH_CHECK_INTERVAL_MILLIS);
      verifyJobsHealthy();
    }

    // Rethrow test failure, if any
    try {
      future.get();
    } catch (ExecutionException e) {
      final Throwable cause = (e.getCause() == null) ? e : e.getCause();
      throw Throwables.propagate(cause);
    }
  }

  private void verifyJobsHealthy() throws AssertionError {
    for (TemporaryJob job : jobs) {
      job.verifyHealthy();
    }
  }

  /**
   * Undeploys and deletes jobs leftover from previous runs of TemporaryJobs. This would happen if
   * the test was terminated before the cleanup code was called. This method will iterate over each
   * file in the specified directory. Each filename is the prefix that was used for job names
   * during previous runs. The method will undeploy and delete any jobs that have a matching
   * prefix, and the delete the file. If the file is locked, it is currently in use, and will be
   * skipped.
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   */
  private void removeOldJobs(final Path prefixDirectory)
      throws ExecutionException, InterruptedException, IOException {
    final File[] files = prefixDirectory.toFile().listFiles();
    if (files == null || files.length == 0) {
      return;
    }

    log.info("Removing old temporary jobs");

    final Map<JobId, Job> jobs = client.jobs().get();

    // Iterate over all files in the directory
    for (File file : files) {
      // Skip .tmp files which are generated when JobPrefixFiles are created. Also skip
      // directories. We don't expect any, but skip them just in case.
      if (file.getName().endsWith(".tmp") || file.isDirectory()) {
        continue;
      }
      // If we can't obtain a lock for the file, it either has already been deleted, or is being
      // used by another process. In either case, skip over it.
      try (
        JobPrefixFile prefixFile = JobPrefixFile.tryFromExistingFile(file.toPath())
      ) {
        if (prefixFile == null) {
          log.debug("Unable to create JobPrefixFile for {}", file.getPath());
          continue;
        }

        boolean jobRemovalFailed = false;
        // Iterate over jobs, looking for ones with a matching prefix.
        for (Map.Entry<JobId, Job> entry : jobs.entrySet()) {
          final JobId jobId = entry.getKey();
          // Skip over job if the id doesn't start with current filename.
          if (!jobId.getName().startsWith(prefixFile.prefix())) {
            continue;
          }
          // Get list of all hosts where this job is deployed, and undeploy
          final JobStatus status = client.jobStatus(entry.getKey()).get();
          final List<String> hosts = ImmutableList.copyOf(status.getDeployments().keySet());
          final List<AssertionError> errors =
              undeploy(client, entry.getValue(), hosts, new ArrayList<AssertionError>());

          // Set flag indicating if any errors occur
          if (!errors.isEmpty()) {
            jobRemovalFailed = true;
          }
        }

        // If all jobs were removed successfully, then delete the prefix file. Otherwise,
        // leave it there so we can try again next time.
        if (!jobRemovalFailed) {
          prefixFile.delete();
        }
      } catch (NoSuchFileException e) {
        log.debug("File {} already processed by somebody else.", file.getPath());
      } catch (Exception e) {
        // log exception and continue on to next file
        log.warn("Exception processing file {}", file.getPath(), e);
      }
    }
  }

  public JobPrefixFile jobPrefixFile() {
    return jobPrefixFile;
  }

  public String prefix() {
    return jobPrefixFile.prefix();
  }

  static Config loadConfig() {
    final ConfigResolveOptions resolveOptions =
        ConfigResolveOptions.defaults().setAllowUnresolved(true);

    final Config baseConfig = ConfigFactory.load(
        "helios-base.conf", ConfigParseOptions.defaults(), resolveOptions);
    log.debug("base config: " + baseConfig);

    final Config appConfig = ConfigFactory.load(
        "helios.conf", ConfigParseOptions.defaults(), resolveOptions);
    log.debug("app config: " + appConfig);

    final Config returnConfig = appConfig.withFallback(baseConfig);
    log.debug("result config: " + returnConfig);

    return returnConfig;
  }

  static String getProfileFromConfig(final Config preConfig) {
    if (preConfig.hasPath(HELIOS_TESTING_PROFILE)) {
      return preConfig.getString(HELIOS_TESTING_PROFILE);
    }
    return null;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(final String profile) {
    return new Builder(profile);
  }

  public static class Builder {

    Builder(final String profile) {
      this(profile, TemporaryJobs.loadConfig());
    }

    Builder() {
      this(TemporaryJobs.loadConfig());
    }

    // I feel like I'm building the y-combinator here because Java insists on the calls to this()
    // in a constructor being the first thing in the method.
    private Builder(final Config preConfig) {
      this(TemporaryJobs.getProfileFromConfig(preConfig), preConfig);
    }

    private Builder(final String profile, final Config preConfig) {
      log.debug("Using profile: " + profile);
      if (profile != null) {
        final String key = HELIOS_TESTING_PROFILES + profile;
        if (preConfig.hasPath(key)) {
          this.config = preConfig.getConfig(key);
        } else {
          throw new RuntimeException("The configuration profile " + profile + " does not exist");
        }
      } else {
        this.config = ConfigFactory.empty();
      }
      if (this.config.hasPath("jobDeployedMessageFormat")) {
        jobDeployedMessageFormat(this.config.getString("jobDeployedMessageFormat"));
      }
      if (this.config.hasPath("user")) {
        user(this.config.getString("user"));
      }
      if (this.config.hasPath("hostFilter")) {
        hostFilter(this.config.getString("hostFilter"));
      }
      if (this.config.hasPath("endpoints")) {
        endpointStrings(getListByKey("endpoints", config));
      }
      if (this.config.hasPath("domain")) {
        domain(this.config.getString("domain"));
      }
      if (this.config.hasPath("hostPickingStrategy")) {
        processHostPickingStrategy();
      }
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

    private final Config config;
    private String user = DEFAULT_USER;
    private Prober prober = DEFAULT_PROBER;
    private Deployer deployer;
    private String hostFilter = System.getenv("HELIOS_HOST_FILTER");
    private HeliosClient client;
    private String prefixDirectory;
    private String jobDeployedMessageFormat = null;
    private HostPickingStrategy hostPickingStrategy = HostPickingStrategies.random();

    public Builder domain(final String domain) {
      return client(HeliosClient.newBuilder()
                        .setUser(user)
                        .setDomain(domain)
                        .build());
    }

    public Builder endpoints(final String... endpoints) {
      return endpointStrings(asList(endpoints));
    }

    public Builder endpointStrings(final List<String> endpoints) {
      return client(HeliosClient.newBuilder()
                        .setUser(user)
                        .setEndpointStrings(endpoints)
                        .build());
    }

    public Builder endpoints(final URI... endpoints) {
      return endpoints(asList(endpoints));
    }

    public Builder endpoints(final List<URI> endpoints) {
      return client(HeliosClient.newBuilder()
                        .setUser(user)
                        .setEndpoints(endpoints)
                        .build());
    }

    public Builder hostPickingStrategy(final HostPickingStrategy strategy) {
      this.hostPickingStrategy = strategy;
      return this;
    }

    public Builder user(final String user) {
      this.user = user;
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

    public Builder deployer(final Deployer deployer) {
      this.deployer = deployer;
      return this;
    }

    public Builder client(final HeliosClient client) {
      this.client = client;
      return this;
    }

    public Builder hostFilter(final String hostFilter) {
      this.hostFilter = hostFilter;
      return this;
    }

    public Builder prefixDirectory(final String prefixDirectory) {
      this.prefixDirectory = prefixDirectory;
      return this;
    }

    public TemporaryJobs build() {
      return new TemporaryJobs(this, config);
    }
  }
}
