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

package com.spotify.helios.master;

import com.spotify.helios.agent.InterruptingScheduledService;
import com.spotify.helios.common.Clock;
import com.spotify.helios.common.SystemClock;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.TaskStatusEvent;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Removes old jobs that haven't been deployed for a while.
 */
public class OldJobReaper extends InterruptingScheduledService {

  private static final Clock SYSTEM_CLOCK = new SystemClock();
  private static final long INTERVAL = 1;
  private static final TimeUnit INTERVAL_TIME_UNIT = TimeUnit.DAYS;

  private static final Logger log = LoggerFactory.getLogger(OldJobReaper.class);

  private final MasterModel masterModel;
  private final long retentionMillis;
  private final Clock clock;

  public OldJobReaper(final MasterModel masterModel, final long retentionDays) {
    this(masterModel, retentionDays, SYSTEM_CLOCK);
  }

  OldJobReaper(final MasterModel masterModel,
               final long retentionDays,
               final Clock clock) {
    this.masterModel = masterModel;
    checkArgument(retentionDays > 0);
    this.retentionMillis = TimeUnit.DAYS.toMillis(retentionDays);
    this.clock = clock;
  }

  @Override
  protected void runOneIteration() {
    log.debug("Reaping old jobs.");

    final Map<JobId, Job> jobs = masterModel.getJobs();
    for (final Map.Entry<JobId, Job> jobEntry : jobs.entrySet()) {
      final JobId jobId = jobEntry.getKey();

      try {
        final List<TaskStatusEvent> events = masterModel.getJobHistory(jobId);

        //noinspection StatementWithEmptyBody
        if (events.isEmpty()) {
          // Don't reap. We're being conservative here in case the agent couldn't write the
          // history or the reaper happens to be running right in between the time the job is
          // created and when it's deployed.
        } else {
          // Get the last event which is the most recent
          final TaskStatusEvent event = events.get(events.size() - 1);
          // Calculate the amount of time in milliseconds that has elapsed since the last event
          final long unusedDurationMillis = clock.now().getMillis() - event.getTimestamp();

          if (unusedDurationMillis > retentionMillis) {
            // Check the job isn't deployed anywhere
            final JobStatus jobStatus = masterModel.getJobStatus(jobId);
            final Map<String, Deployment> deployments = jobStatus.getDeployments();
            if (deployments.size() == 0) {
              try {
                log.info("Reaping old job '{}' (unused for {} days)", jobId,
                         DurationFormatUtils.formatDuration(unusedDurationMillis, "DD H:mm"));
                masterModel.removeJob(jobId, jobEntry.getValue().getToken());
              } catch (Exception e) {
                log.warn("Failed to reap old job '{}'", jobId, e);
              }
            }
          }
        }
      } catch (Exception e) {
        log.warn("Failed to determine if job '{}' should be reaped", jobId, e);
      }
    }
  }

  @Override
  protected ScheduledFuture<?> schedule(final Runnable runnable,
                                        final ScheduledExecutorService executorService) {
    return executorService.scheduleWithFixedDelay(runnable, 0, INTERVAL, INTERVAL_TIME_UNIT);
  }
}
