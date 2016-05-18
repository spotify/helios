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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TemporaryJobs implements TestRule {
  private static final Logger log = LoggerFactory.getLogger(TemporaryJobs.class);

  private static final String HELIOS_TESTING_PROFILE = "helios.testing.profile";
  private static final String HELIOS_TESTING_PROFILES = "helios.testing.profiles.";
  private static final Prober DEFAULT_PROBER = new Prober();
  private static final String DEFAULT_TEST_REPORT_DIRECTORY = "target/helios-reports/test";
  private static final long JOB_HEALTH_CHECK_INTERVAL_MILLIS = SECONDS.toMillis(5);
  private static final long DEFAULT_DEPLOY_TIMEOUT_MILLIS = MINUTES.toMillis(10);

  private static volatile HeliosSoloDeployment soloDeployment;

  private final HeliosClient client;
  private final Prober prober;
  private final Config config;
  private final Map<String, String> env;
  private final List<TemporaryJob> jobs = Lists.newCopyOnWriteArrayList();
  private final Deployer deployer;

  private final TemporaryJobReports reports;
  private final ThreadLocal<TemporaryJobReports.ReportWriter> reportWriter;

  private static synchronized HeliosSoloDeployment getOrCreateHeliosSoloDeployment() {
    if (soloDeployment == null) {
      // TODO (dxia) remove checkForNewImages(). Set here to prevent using
      // spotify/helios-solo:latest from docker hub
      soloDeployment = HeliosSoloDeployment.fromEnv().checkForNewImages(false).build();
    }
    return soloDeployment;
  }

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
    this.env = checkNotNull(builder.env, "env");

    checkArgument(builder.deployTimeoutMillis >= 0, "deployTimeoutMillis");

    this.deployer = fromNullable(builder.deployer).or(
        new DefaultDeployer(client, jobs, builder.jobDeployedMessageFormat,
                            builder.deployTimeoutMillis));

    final Path testReportDirectory = Paths.get(fromNullable(builder.testReportDirectory)
                                                   .or(DEFAULT_TEST_REPORT_DIRECTORY));
    this.reports = new TemporaryJobReports(testReportDirectory);
    this.reportWriter = new ThreadLocal<TemporaryJobReports.ReportWriter>() {
      @Override
      protected TemporaryJobReports.ReportWriter initialValue() {
        log.warn("unable to determine test context, writing event log to stdout");
        return TemporaryJobs.this.reports.getWriterForStream(System.out);
      }
    };

    this.config = config;
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
    }

    final List<AssertionError> errors = newArrayList();

    for (final TemporaryJob job : jobs) {
      jobIds.add(job.job().getId());
      // TODO (dxia) Undeploying a job doesn't guaruntee the container will be stopped immediately.
      // Luckily TaskRunner tells docker to wait 120 seconds after stopping to kill the container.
      // So containers that don't immediately stop **should** only stay around for at most 120
      // seconds.
      job.undeploy(errors);
    }

    for (final TemporaryJobReports.Step step : undeploy.asSet()) {
      step.tag("jobs", jobIds);
    }

    for (final AssertionError error : errors) {
      log.error(error.getMessage());
    }

    if (errors.isEmpty()) {
      for (final TemporaryJobReports.Step step : undeploy.asSet()) {
        step.markSuccess();
      }
    }

    for (final TemporaryJobReports.Step step : undeploy.asSet()) {
      step.finish();
    }

    // We don't close soloDeployment because other callers may still be using the singleton.
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
        deployer, prober, env, reportWriter.get(), jobBuilder);

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
    if (config.hasPath("imageInfoFile")) {
      builder.imageFromInfoFile(config.getString("imageInfoFile"));
    }
    if (config.hasPath("registrationDomain")) {
      builder.registrationDomain(config.getString("registrationDomain"));
    }
    // port and expires intentionally left out -- since expires is a specific point in time, I
    // cannot imagine a config-file use for it, additionally for ports, I'm thinking that port
    // allocations are not likely to be common -- but PR's welcome if I'm wrong. - drewc@spotify.com
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
   * has master ZooKeeper, and one agent.
   *
   * @return an instance of TemporaryJobs
   * @see <a href=
   * "https://github.com/spotify/helios/blob/master/docs/testing_framework.md#configuration-by-file"
   * >Helios Testing Framework - Configuration By File</a>
   */
  public static TemporaryJobs create() {
    return builder().build();
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
          throw Throwables.propagate(e);
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
    for (final TemporaryJob job : jobs) {
      job.verifyHealthy();
    }
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
    return new Builder(profile, HeliosConfig.loadConfig("helios-testing"), env);
  }

  public static class Builder {

    private final Map<String, String> env;
    private final Config config;
    private Prober prober = DEFAULT_PROBER;
    private Deployer deployer;
    private HeliosClient client;
    private String testReportDirectory;
    private String jobDeployedMessageFormat;
    private long deployTimeoutMillis = DEFAULT_DEPLOY_TIMEOUT_MILLIS;

    Builder(final String profile, final Config rootConfig, final Map<String, String> env) {
      this.env = env;

      if (profile == null) {
        this.config = HeliosConfig.getDefaultProfile(
            HELIOS_TESTING_PROFILE, HELIOS_TESTING_PROFILES, rootConfig);
      } else {
        this.config = HeliosConfig.getProfile(HELIOS_TESTING_PROFILES, profile, rootConfig);
      }

      if (this.config.hasPath("jobDeployedMessageFormat")) {
        jobDeployedMessageFormat(this.config.getString("jobDeployedMessageFormat"));
      }
      if (this.config.hasPath("deployTimeoutMillis")) {
        deployTimeoutMillis(this.config.getLong("deployTimeoutMillis"));
      }

      client = getOrCreateHeliosSoloDeployment().client();
    }

    public Builder jobDeployedMessageFormat(final String jobLinkFormat) {
      this.jobDeployedMessageFormat = jobLinkFormat;
      return this;
    }

    public Builder testReportDirectory(final String testReportDirectory) {
      this.testReportDirectory = testReportDirectory;
      return this;
    }

    public Builder deployTimeoutMillis(final long timeout) {
      this.deployTimeoutMillis = timeout;
      return this;
    }

    /**
     * Used to configure a custom prober for testing. When testing, we often don't care if a port
     * is actually open. So we just use a mock prober that returns true.
     * @param prober {@link Prober}
     * @return This Builder, with the prober configured.
     */
    @VisibleForTesting
    Builder prober(final Prober prober) {
      this.prober = prober;
      return this;
    }

    /**
     * Used to configure a custom deployer for testing. When testing, we often don't care if a job
     * is actually deployed. So we just use a mock deployer that returns true.
     * @param deployer {@link Deployer}
     * @return This Builder, with the deployer configured.
     */
    @VisibleForTesting
    Builder deployer(final Deployer deployer) {
      this.deployer = deployer;
      return this;
    }

    public TemporaryJobs build() {
      return new TemporaryJobs(this, config);
    }
  }
}
