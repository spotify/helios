/*
 * Copyright (c) 2016 Spotify AB.
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

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.docker.client.DockerClient.LogsParam.follow;
import static com.spotify.docker.client.DockerClient.LogsParam.stderr;
import static com.spotify.docker.client.DockerClient.LogsParam.stdout;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class HeliosSoloLogService extends AbstractScheduledService {

  private static final Logger log = LoggerFactory.getLogger(HeliosSoloLogService.class);

  private final HeliosClient heliosClient;
  private final DockerClient dockerClient;
  private final LogStreamProvider logStreamProvider;

  private Map<String, Future> logFutures = Maps.newHashMap();

  HeliosSoloLogService(@NotNull final HeliosClient heliosClient,
                       @NotNull final DockerClient dockerClient,
                       @NotNull final LogStreamProvider logStreamProvider) {
    super();
    this.heliosClient = heliosClient;
    this.dockerClient = dockerClient;
    this.logStreamProvider = logStreamProvider;
  }

  @Override
  protected void runOneIteration() throws Exception {
    try {
      // fetch all the jobs running on the solo deployment
      for (final String host : heliosClient.listHosts().get()) {
        final HostStatus hostStatus = heliosClient.hostStatus(host).get();
        final Map<JobId, TaskStatus> statuses = hostStatus.getStatuses();

        for (final TaskStatus status : statuses.values()) {
          final JobId jobId = status.getJob().getId();
          final String containerId = status.getContainerId();
          if (isNullOrEmpty(containerId)) {
            continue;
          }

          if (!logFutures.containsKey(containerId)) {
            // for any containers we're not already tracking, attach to their stdout/stderr
            final Future<?> future = this.executor().submit(new Runnable() {
              @Override
              public void run() {
                try {
                  final LogStream logStream = dockerClient.logs(containerId, stdout(), stderr(),
                                                                follow());

                  log.info("attaching stdout/stderr for job={}, container={}", jobId, containerId);
                  logStream.attach(logStreamProvider.getStdoutStream(jobId, containerId),
                                   logStreamProvider.getStderrStream(jobId, containerId));
                } catch (final InterruptedException e) {
                  // we're shutting down, no need to log anything
                } catch (final Exception e) {
                  log.warn("error streaming log for job={}, container={} - {}", jobId, containerId,
                           e);
                }
              }
            });

            logFutures.put(containerId, future);
          }
        }
      }
    } catch (Exception e) {
      // do nothing, we'll retry in a bit
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

}
