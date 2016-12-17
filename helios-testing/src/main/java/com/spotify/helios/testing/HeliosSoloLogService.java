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

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.docker.client.DockerClient.LogsParam.follow;
import static com.spotify.docker.client.DockerClient.LogsParam.stderr;
import static com.spotify.docker.client.DockerClient.LogsParam.stdout;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.http.ConnectionClosedException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HeliosSoloLogService extends AbstractScheduledService {

  private static final Logger log = LoggerFactory.getLogger(HeliosSoloLogService.class);

  private final HeliosClient heliosClient;
  private final DockerClient dockerClient;
  private final LogStreamFollower logStreamFollower;

  private Map<String, Future> logFutures = Maps.newHashMap();

  HeliosSoloLogService(@NotNull final HeliosClient heliosClient,
                       @NotNull final DockerClient dockerClient,
                       @NotNull final LogStreamFollower logStreamFollower) {
    super();
    this.heliosClient = heliosClient;
    this.dockerClient = dockerClient;
    this.logStreamFollower = logStreamFollower;
  }

  private static <T> T get(Future<T> future)
      throws InterruptedException, ExecutionException, TimeoutException {
    return future.get(1, TimeUnit.SECONDS);
  }

  @Override
  protected ScheduledExecutorService executor() {
    final ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat(serviceName())
        .setDaemon(true)
        .build();
    return Executors.newSingleThreadScheduledExecutor(threadFactory);
  }

  @Override
  protected void runOneIteration() throws Exception {
    try {
      // fetch all the jobs running on the solo deployment
      for (final String host : get(heliosClient.listHosts())) {
        final HostStatus hostStatus = get(heliosClient.hostStatus(host));
        if (hostStatus == null) {
          continue;
        }

        final Map<JobId, TaskStatus> statuses = hostStatus.getStatuses();

        for (final TaskStatus status : statuses.values()) {
          final JobId jobId = status.getJob().getId();
          final String containerId = status.getContainerId();
          if (isNullOrEmpty(containerId)) {
            continue;
          }

          if (!logFutures.containsKey(containerId)) {
            // for any containers we're not already tracking, attach to their stdout/stderr
            final Future<?> future = this.executor().submit(new LogFollowJob(containerId, jobId));
            logFutures.put(containerId, future);
          }
        }
      }
    } catch (Exception e) {
      // Ignore TimeoutException as that is to be expected sometimes
      if (!(Throwables.getRootCause(e) instanceof TimeoutException)) {
        log.warn("Caught exception, will ignore", e);
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    for (final Future future : logFutures.values()) {
      future.cancel(true);
    }

    super.shutDown();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(0, 100, TimeUnit.MILLISECONDS);
  }

  private class LogFollowJob implements Callable<Void> {

    private final String containerId;
    private final JobId jobId;

    private LogFollowJob(final String containerId, final JobId jobId) {
      this.containerId = containerId;
      this.jobId = jobId;
    }

    @Override
    public Void call() throws IOException, DockerException {
      try (final LogStream logStream =
               dockerClient.logs(containerId, stdout(), stderr(), follow())) {
        log.info("attaching stdout/stderr for job={}, container={}", jobId, containerId);
        logStreamFollower.followLog(jobId, containerId, logStream);
      } catch (InterruptedException e) {
        // Ignore
      } catch (final Throwable t) {
        if (!(Throwables.getRootCause(t) instanceof ConnectionClosedException)) {
          log.warn("error streaming log for job={}, container={}", jobId, containerId, t);
        }
        throw t;
      }
      return null;
    }
  }
}
