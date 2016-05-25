/*
 * Copyright (c) 2015 Spotify AB.
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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.System.getProperty;
import static java.util.Arrays.asList;

/**
 * This class represents an existing Helios cluster.
 */
public class ExistingHeliosDeployment implements HeliosDeployment {

  private static final Logger log = LoggerFactory.getLogger(ExistingHeliosDeployment.class);

  private static final String DEFAULT_USER = getProperty("user.name");
  private static final String DEFAULT_PREFIX_DIRECTORY = "/tmp/helios-temp-jobs";

  private final HeliosClient heliosClient;
  private final boolean existingHeliosClient;
  private final JobPrefixFile jobPrefixFile;
  private final Undeployer undeployer;

  private ExistingHeliosDeployment(final Builder builder) {
    this.heliosClient = checkNotNull(builder.heliosClient, "heliosClient");
    this.existingHeliosClient = builder.existingHeliosClient;
    final Path prefixDirectory = Paths.get(firstNonNull(builder.prefixDirectory,
                                                        DEFAULT_PREFIX_DIRECTORY));

    try {
      removeOldJobs(asList(firstNonNull(prefixDirectory.toFile().listFiles(), new File[0])));
      if (isNullOrEmpty(builder.jobPrefix)) {
        this.jobPrefixFile = JobPrefixFile.create(prefixDirectory);
      } else {
        this.jobPrefixFile = JobPrefixFile.create(builder.jobPrefix, prefixDirectory);
      }
    } catch (IOException | ExecutionException | InterruptedException e) {
      throw Throwables.propagate(e);
    }

    this.undeployer = firstNonNull(builder.undeployer, new DefaultUndeployer(heliosClient));
  }

  @Override
  public HeliosClient client() {
    return heliosClient;
  }

  @Override
  public Set<URI> uris() {
    return Collections.emptySet();
  }

  /**
   * This method will close the associated {@link HeliosClient} if it's a new instance created by
   * this class. Do not call this method if you passed in a HeliosClient and still need to use it.
   */
  @Override
  public void close() {
    if (!existingHeliosClient) {
      try {
        heliosClient.close();
      } catch (IOException e) {
        log.warn("HeliosClient did not close cleanly: {}", e.toString());
      }
    }

    jobPrefixFile.delete();
  }

  public static Builder newBuilder() {
    return new Builder();
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
  @VisibleForTesting
  void removeOldJobs(final List<File> files)
      throws ExecutionException, InterruptedException, IOException {
    if (files == null || files.size() == 0) {
      return;
    }

    log.info("Removing old temporary jobs");

    final Map<JobId, Job> jobs = heliosClient.jobs().get();

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
          final JobPrefixFile prefixFile = JobPrefixFile.tryFromExistingFile(file.toPath())
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
          final JobStatus status = heliosClient.jobStatus(entry.getKey()).get();
          final List<String> hosts = ImmutableList.copyOf(status.getDeployments().keySet());
          final List<AssertionError> errors = undeployer.undeploy(entry.getValue(), hosts);

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

  public static class Builder {
    private String user = DEFAULT_USER;
    private HeliosClient heliosClient;
    private boolean existingHeliosClient = false;
    private String prefixDirectory;
    private String jobPrefix;
    private Undeployer undeployer;

    public Builder domain(final String domain) {
      this.heliosClient = HeliosClient.newBuilder()
          .setUser(user)
          .setDomain(domain)
          .build();
      return this;
    }

    public Builder endpointStrings(final List<String> endpoints) {
      this.heliosClient = HeliosClient.newBuilder()
          .setUser(user)
          .setEndpointStrings(endpoints)
          .build();
      return this;
    }

    public Builder endpoints(final List<URI> endpoints) {
      this.heliosClient = HeliosClient.newBuilder()
          .setUser(user)
          .setEndpoints(endpoints)
          .build();
      return this;
    }

    public Builder heliosClient(final HeliosClient heliosClient) {
      this.existingHeliosClient = true;
      this.heliosClient = heliosClient;
      return this;
    }

    public Builder prefixDirectory(final String prefixDirectory) {
      this.prefixDirectory = prefixDirectory;
      return this;
    }

    public Builder jobPrefix(final String jobPrefix) {
      this.jobPrefix = jobPrefix;
      return this;
    }

    @VisibleForTesting
    Builder undeployer(final Undeployer undeployer) {
      this.undeployer = undeployer;
      return this;
    }

    public ExistingHeliosDeployment build() {
      return new ExistingHeliosDeployment(this);
    }
  }

}
