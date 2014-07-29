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

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.containsPattern;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.spotify.helios.testing.Jobs.undeploy;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.fail;

public class TemporaryJobs extends ExternalResource {

  private static final Logger log = LoggerFactory.getLogger(TemporaryJob.class);

  private static final String DEFAULT_USER = System.getProperty("user.name");
  private static final Prober DEFAULT_PROBER = new DefaultProber();
  private static final String DEFAULT_LOCAL_HOST_FILTER = ".*";
  private static final String DEFAULT_HOST_FILTER = System.getenv("HELIOS_HOST_FILTER");
  private static final String DEFAULT_PREFIX_DIRECTORY = "/tmp/helios-temp-jobs";

  private final HeliosClient client;
  private final Prober prober;
  private final String defaultHostFilter;
  private final JobPrefixFile jobPrefixFile;

  private final List<TemporaryJob> jobs = newArrayList();

  private final TemporaryJob.Deployer deployer = new TemporaryJob.Deployer() {
    @Override
    public TemporaryJob deploy(final Job job, final String hostFilter,
                               final Set<String> waitPorts) {
      if (isNullOrEmpty(hostFilter)) {
        fail("a host filter pattern must be passed to hostFilter(), " +
             "or one must be specified in HELIOS_HOST_FILTER");
      }

      final List<String> hosts;
      try {
        log.info("Getting list of hosts");

        hosts = client.listHosts().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new AssertionError("Failed to get list of Helios hosts", e);
      }

      final List<String> filteredHosts = FluentIterable.from(hosts)
          .filter(containsPattern(hostFilter))
          .toList();

      if (filteredHosts.isEmpty()) {
        fail(format("no hosts matched the filter pattern - %s", hostFilter));
      }

      final String chosenHost = filteredHosts.get(new Random().nextInt(filteredHosts.size()));
      return deploy(job, asList(chosenHost), waitPorts);
    }

    @Override
    public TemporaryJob deploy(final Job job, final List<String> hosts,
                               final Set<String> waitPorts) {
      if (!started) {
        fail("deploy() must be called in a @Before or in the test method");
      }

      if (hosts.isEmpty()) {
        fail("at least one host must be explicitly specified, or deploy() must be called with " +
             "no arguments to automatically select a host");
      }

      log.info("Deploying {} to {}", job.getImage(), Joiner.on(", ").skipNulls().join(hosts));
      final TemporaryJob temporaryJob = new TemporaryJob(client, prober, job, hosts, waitPorts);
      jobs.add(temporaryJob);
      temporaryJob.deploy();
      return temporaryJob;
    }
  };

  private boolean started;

  TemporaryJobs(final Builder builder) {
    this.client = checkNotNull(builder.client, "client");
    this.prober = checkNotNull(builder.prober, "prober");
    this.defaultHostFilter = checkNotNull(builder.hostFilter, "hostFilter");
    final Path prefixDirectory = Paths.get(Optional.fromNullable(builder.prefixDirectory)
        .or(DEFAULT_PREFIX_DIRECTORY));

    try {
      removeOldJobs(prefixDirectory);
      this.jobPrefixFile = JobPrefixFile.create(prefixDirectory);
    } catch (IOException | ExecutionException | InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  public TemporaryJobBuilder job() {
    return new TemporaryJobBuilder(deployer, jobPrefixFile.prefix())
        .hostFilter(defaultHostFilter);
  }

  /**
   * Creates a new instance of TemporaryJobs. Will attempt to connect to a helios master at
   * http://localhost:5801 by default. This can be overridden by setting one of two environment
   * variables.
   * <ul>
   * <li>HELIOS_DOMAIN - any domain which contains a helios master</li>
   * <li>HELIOS_ENDPOINTS - a comma separated list of helios master endpoints</li>
   * </ul>
   * If both variables are set, HELIOS_DOMAIN will take precedence.
   * @return an instance of TemporaryJobs
   */

  public static TemporaryJobs create() {
    final String domain = System.getenv("HELIOS_DOMAIN");
    if (!isNullOrEmpty(domain)) {
      return create(domain);
    }
    final String endpoints = System.getenv("HELIOS_ENDPOINTS");
    final Builder builder = builder();
    if (!isNullOrEmpty(endpoints)) {
      builder.endpointStrings(Splitter.on(',').splitToList(endpoints));
    } else {
      // We're running locally
      builder.hostFilter(Optional.fromNullable(DEFAULT_HOST_FILTER).or(DEFAULT_LOCAL_HOST_FILTER));
      builder.endpoints("http://localhost:5801");
    }
    return builder.build();
  }

  public static TemporaryJobs create(final HeliosClient client) {
    return builder().client(client).build();
  }

  public static TemporaryJobs create(final String domain) {
    return builder().domain(domain).build();
  }

  @Override
  protected void before() throws Throwable {
    started = true;
  }

  @Override
  protected void after() {
    final List<AssertionError> errors = newArrayList();

    log.info("Undeploying temporary jobs");

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
    if (files == null) {
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
          log.info("Getting status for job {}", jobId);
          final JobStatus status = client.jobStatus(entry.getKey()).get();
          final List<String> hosts = ImmutableList.copyOf(status.getDeployments().keySet());

          log.info("Undeploying job {} from hosts {}",
                   jobId,
                   Joiner.on(", ").skipNulls().join(hosts));

          final List<AssertionError> errors =
              undeploy(client, jobId, hosts, new ArrayList<AssertionError>());

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

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    Builder() {
    }

    private String user = DEFAULT_USER;
    private Prober prober = DEFAULT_PROBER;
    private String hostFilter = DEFAULT_HOST_FILTER;
    private HeliosClient client;
    private String prefixDirectory;

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

    public Builder user(final String user) {
      this.user = user;
      return this;
    }

    public Builder prober(final Prober prober) {
      this.prober = prober;
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
      return new TemporaryJobs(this);
    }
  }
}
