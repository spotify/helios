/*-
 * -\-\-
 * Helios Testing Library
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

package com.spotify.helios.testing;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.spotify.helios.testing.Jobs.undeploy;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import com.typesafe.config.ConfigValueType;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
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
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TemporaryJobs implements TestRule {
  private static final Logger log = LoggerFactory.getLogger(TemporaryJobs.class);

  static final String HELIOS_TESTING_PROFILE = "helios.testing.profile";
  private static final String HELIOS_TESTING_PROFILES = "helios.testing.profiles.";
  private static final String DEFAULT_USER = getProperty("user.name");
  private static final Prober DEFAULT_PROBER = new DefaultProber();
  private static final String DEFAULT_LOCAL_HOST_FILTER = ".+";
  private static final String DEFAULT_PREFIX_DIRECTORY = "/tmp/helios-temp-jobs";
  private static final String DEFAULT_TEST_REPORT_DIRECTORY = "target/helios-reports/test";
  private static final long JOB_HEALTH_CHECK_INTERVAL_MILLIS = SECONDS.toMillis(5);
  private static final long DEFAULT_DEPLOY_TIMEOUT_MILLIS = MINUTES.toMillis(10);

  private final HeliosClient client;
  private final Prober prober;
  private final String defaultHostFilter;
  private final JobPrefixFile jobPrefixFile;
  private final Config config;
  private final Map<String, String> env;
  private final List<TemporaryJob> jobs = Lists.newCopyOnWriteArrayList();
  private final Deployer deployer;
  private final TemporaryJobReports reports;
  private final ThreadLocal<TemporaryJobReports.ReportWriter> reportWriter;

  private boolean removedOldJobs = false;

  private final ExecutorService executor = MoreExecutors.getExitingExecutorService(
      (ThreadPoolExecutor) Executors.newFixedThreadPool(
          1, new ThreadFactoryBuilder()
              .setNameFormat("helios-test-runner-%d")
              .setDaemon(true)
              .build()),
      0, SECONDS);
  private final Path prefixDirectory;

  TemporaryJobs(final Builder builder, final Config config) {
    this.client = checkNotNull(builder.client, "client");
    this.prober = checkNotNull(builder.prober, "prober");
    this.defaultHostFilter = checkNotNull(builder.hostFilter, "hostFilter");
    this.env = checkNotNull(builder.env, "env");

    checkArgument(builder.deployTimeoutMillis >= 0, "deployTimeoutMillis");

    this.deployer = fromNullable(builder.deployer).or(
        new DefaultDeployer(client, jobs, builder.hostPickingStrategy,
            builder.jobDeployedMessageFormat, builder.deployTimeoutMillis));

    prefixDirectory = Paths.get(fromNullable(builder.prefixDirectory)
        .or(DEFAULT_PREFIX_DIRECTORY));
    try {
      if (isNullOrEmpty(builder.jobPrefix)) {
        this.jobPrefixFile = JobPrefixFile.create(prefixDirectory);
      } else {
        this.jobPrefixFile = JobPrefixFile.create(builder.jobPrefix, prefixDirectory);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (builder.reports == null) {
      final Path testReportDirectory = Paths.get(
          fromNullable(builder.testReportDirectory).or(DEFAULT_TEST_REPORT_DIRECTORY));
      this.reports = new TemporaryJobJsonReports(testReportDirectory);
    } else {
      this.reports = builder.reports;
    }

    this.reportWriter = new ThreadLocal<TemporaryJobReports.ReportWriter>() {
      @Override
      protected TemporaryJobReports.ReportWriter initialValue() {
        log.warn("unable to determine test context, writing to default event log");
        return TemporaryJobs.this.reports.getDefaultWriter();
      }
    };

    // Load in the prefix so it can be used in the config
    final Config configWithPrefix = ConfigFactory.empty()
        .withValue("prefix", ConfigValueFactory.fromAnyRef(prefix()));

    this.config = config.withFallback(configWithPrefix).resolve();
  }

  /**
   * Perform setup. This is normally called by JUnit when TemporaryJobs is used with @Rule.
   * If @Rule cannot be used, call this method before calling {@link #job()}.
   *
   * <p>Note: When not being used as a @Rule, jobs will not be monitored during test run.
   */
  public void before() {
    deployer.readyToDeploy();
  }

  /**
   * Perform teardown. This is normally called by JUnit when TemporaryJobs is used with @Rule.
   * If @Rule cannot be used, call this method after running tests.
   */
  public void after() {
    after(Optional.<TemporaryJobReports.ReportWriter>absent());
  }

  void after(final Optional<TemporaryJobReports.ReportWriter> writer) {
    final Optional<TemporaryJobReports.Step> undeploy = writer
        .transform(new Function<TemporaryJobReports.ReportWriter, TemporaryJobReports.Step>() {
          @Override
          public TemporaryJobReports.Step apply(final TemporaryJobReports.ReportWriter writer) {
            return writer.step("undeploy");
          }
        });
    final List<JobId> jobIds = Lists.newArrayListWithCapacity(jobs.size());

    // Stop the test runner thread
    executor.shutdownNow();
    try {
      final boolean terminated = executor.awaitTermination(30, SECONDS);
      if (!terminated) {
        log.warn("Failed to stop test runner thread");
      }
    } catch (InterruptedException ignore) {
      // ignored
    }

    final List<AssertionError> errors = newArrayList();

    for (final TemporaryJob job : jobs) {
      jobIds.add(job.job().getId());
      job.undeploy(errors);
    }

    for (final TemporaryJobReports.Step step : undeploy.asSet()) {
      step.tag("jobs", jobIds);
    }

    for (final AssertionError error : errors) {
      log.error(error.getMessage());
    }

    // Don't delete the prefix file if any errors occurred during undeployment, so that we'll
    // try to undeploy them the next time TemporaryJobs is run.
    if (errors.isEmpty()) {
      jobPrefixFile.delete();
      for (final TemporaryJobReports.Step step : undeploy.asSet()) {
        step.markSuccess();
      }
    }

    for (final TemporaryJobReports.Step step : undeploy.asSet()) {
      step.finish();
    }
  }

  public TemporaryJobBuilder job() {
    return this.job(Job.newBuilder());
  }

  private TemporaryJobBuilder job(final Job.Builder jobBuilder) {
    final TemporaryJobBuilder builder = new TemporaryJobBuilder(
        deployer, jobPrefixFile.prefix(), prober, env, reportWriter.get(), jobBuilder);

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
   *
   * @see <a href="https://github.com/spotify/helios/blob/master/docs/testing_framework.md#configuration-by-file">Helios Testing Framework - Configuration By File</a>
   */
  public static TemporaryJobs create() {
    return builder().build();
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
  public Statement apply(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        final TemporaryJobReports.ReportWriter writer = reports.getWriterForTest(description);
        reportWriter.set(writer);

        final TemporaryJobReports.Step test = writer.step("test");
        before();
        removeOldJobs();
        try {
          perform(base, writer);
          test.markSuccess();
        } finally {
          after(Optional.of(writer));

          test.finish();
          writer.close();
          reportWriter.set(null);
        }
      }
    };
  }

  private void perform(final Statement base, final TemporaryJobReports.ReportWriter writer)
      throws InterruptedException {
    // Run the actual test on a thread
    final Future<Object> future = executor.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        reportWriter.set(writer);

        try {
          base.evaluate();
        } catch (MultipleFailureException e) {
          // Log the stack trace for each exception in the MultipleFailureException, because
          // stack traces won't be logged when this is caught and logged higher in the call stack.
          final List<Throwable> failures = e.getFailures();
          log.error(format("MultipleFailureException contains %d failures:", failures.size()));
          for (int i = 0; i < failures.size(); i++) {
            log.error(format("MultipleFailureException %d:", i), failures.get(i));
          }
          throw new RuntimeException(e);
        } catch (Throwable throwable) {
          Throwables.throwIfUnchecked(throwable);
          throw new RuntimeException(throwable);
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
      throw new RuntimeException(cause);
    }
  }

  private void verifyJobsHealthy() throws AssertionError {
    for (final TemporaryJob job : jobs) {
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
   */
  private void removeOldJobs()
      throws ExecutionException, InterruptedException, IOException {

    // only perform cleanup once per TemporaryJobs instance; do not redo cleanup if TemporaryJobs
    // is used as a @Rule in a test class with many test methods
    if (removedOldJobs) {
      return;
    }

    final File[] files = prefixDirectory.toFile().listFiles();
    if (files == null || files.length == 0) {
      return;
    }

    log.info("Removing old temporary jobs");

    final Map<JobId, Job> jobs = client.jobs().get();

    // Iterate over all files in the directory
    for (final File file : files) {
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
        for (final Map.Entry<JobId, Job> entry : jobs.entrySet()) {
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

    removedOldJobs = true;
  }

  public JobPrefixFile jobPrefixFile() {
    return jobPrefixFile;
  }

  public String prefix() {
    return jobPrefixFile.prefix();
  }

  public HeliosClient client() {
    return client;
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
    return builder(profile, env, HeliosClient.newBuilder());
  }

  static Builder builder(final String profile, final Map<String, String> env,
                         final HeliosClient.Builder clientBuilder) {
    return new Builder(profile, HeliosConfig.loadConfig(), env, clientBuilder);
  }

  public static class Builder {

    private final Map<String, String> env;
    private final Config config;
    private String user = DEFAULT_USER;
    private Prober prober = DEFAULT_PROBER;
    private Deployer deployer;
    private String hostFilter;
    private HeliosClient.Builder clientBuilder;
    private HeliosClient client;
    private String prefixDirectory;
    private String testReportDirectory;
    private String jobPrefix;
    private String jobDeployedMessageFormat;
    private HostPickingStrategy hostPickingStrategy = HostPickingStrategies.randomOneHost();
    private long deployTimeoutMillis = DEFAULT_DEPLOY_TIMEOUT_MILLIS;
    private TemporaryJobReports reports;

    Builder(String profile, Config rootConfig, Map<String, String> env,
            HeliosClient.Builder clientBuilder) {
      this.env = env;
      this.clientBuilder = clientBuilder;

      if (profile == null) {
        this.config = HeliosConfig.getDefaultProfile(
            HELIOS_TESTING_PROFILE, HELIOS_TESTING_PROFILES, rootConfig);
      } else {
        this.config = HeliosConfig.getProfile(HELIOS_TESTING_PROFILES, profile, rootConfig);
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
      if (this.config.hasPath("deployTimeoutMillis")) {
        deployTimeoutMillis(this.config.getLong("deployTimeoutMillis"));
      }

      // Configuration from profile may be overridden by environment variables
      configureWithEnv();
    }

    private void configureWithEnv() {
      // Use HELIOS_HOST_FILTER if set
      final String heliosHostFilter = env.get("HELIOS_HOST_FILTER");
      if (heliosHostFilter != null) {
        hostFilter(heliosHostFilter);
      }

      // Use HELIOS_DOMAIN if set
      final String domain = env.get("HELIOS_DOMAIN");
      if (!isNullOrEmpty(domain)) {
        domain(domain);
        return;
      }

      // Use HELIOS_ENDPOINTS if set
      final String endpoints = env.get("HELIOS_ENDPOINTS");
      if (!isNullOrEmpty(endpoints)) {
        endpointStrings(Splitter.on(',').splitToList(endpoints));
        return;
      }

      // If we get here and client is set, we know which master we'll be talking to, so just return
      // as rest of this method handles the case where the helios master wasn't specified.
      if (client != null) {
        return;
      }

      // If we get here, we did not create a client based on environment variables or a testing
      // profile, so check if DOCKER_HOST is set. If so, try to connect to that host on port 5801,
      // assuming it has a helios master running. If not, attempt to connect to
      // http://localhost:5801 as a last attempt.
      final String dockerHost = env.get("DOCKER_HOST");
      if (dockerHost == null) {
        endpoints("http://localhost:5801");
      } else if (!dockerHost.startsWith("unix://")) {
        try {
          final URI uri = new URI(dockerHost);
          endpoints("http://" + uri.getHost() + ":5801");
        } catch (URISyntaxException e) {
          throw new RuntimeException(e);
        }
      }

      // We usually require the caller to specify a host filter, so jobs aren't accidentally
      // deployed to arbitrary hosts. But at this point the master is either running on localhost
      // or the docker host. Either way, this is probably a test machine with one master and one
      // agent both running on the same box, so it is safe to provide a default filter that will
      // deploy anywhere.
      if (isNullOrEmpty(hostFilter)) {
        hostFilter(DEFAULT_LOCAL_HOST_FILTER);
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
        throw new RuntimeException(
            "The hostPickingStrategy " + value + " is not valid. "
            + "Valid values are [random, onerandom, deterministic, onedeterministic] and the "
            + "deterministic variants require a string value hostPickingStrategyKey to be set "
            + "which is used to seed the random number generator, so can be any string.");
      }
    }

    private void verifyHasStrategyKey(final String value) {
      if (!this.config.hasPath("hostPickingStrategyKey")) {
        throw new RuntimeException(
            "host picking strategy [" + value + "] selected but no "
            + "value for hostPickingStrategyKey which is used to seed the random number generator");
      }
    }

    public Builder domain(final String domain) {
      return client(clientBuilder.setUser(user)
          .setDomain(domain)
          .build());
    }

    public Builder endpoints(final String... endpoints) {
      return endpointStrings(asList(endpoints));
    }

    public Builder endpoints(final URI... endpoints) {
      return endpoints(asList(endpoints));
    }

    public Builder endpoints(final List<URI> endpoints) {
      return client(clientBuilder.setUser(user)
          .setEndpoints(endpoints)
          .build());
    }

    public Builder endpointStrings(final List<String> endpoints) {
      return client(clientBuilder.setUser(user)
          .setEndpointStrings(endpoints)
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

    public Builder reports(final TemporaryJobReports reports) {
      this.reports = reports;
      return this;
    }

    public Builder testReportDirectory(final String testReportDirectory) {
      this.testReportDirectory = testReportDirectory;
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
