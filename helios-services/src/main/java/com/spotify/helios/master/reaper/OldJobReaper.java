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

package com.spotify.helios.master.reaper;

import com.spotify.helios.common.Clock;
import com.spotify.helios.common.SystemClock;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.master.MasterModel;

import com.google.common.annotations.VisibleForTesting;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Removes old jobs that haven't been deployed for a while.
 * The logic for whether a job should be reaped depends on whether it's deployed, its last history
 * event, its creation date, and the specified number of retention days.
 *
 * 1. A job that's deployed should NOT BE reaped regardless of its history or creation date.
 * 2. A job not deployed, with history, and an event before the number of retention days should
 *    BE reaped.
 * 3. A job not deployed, with history, and an event after the number of retention days should NOT
 *    BE reaped. An example is a job created a long time ago but deployed recently.
 *    Unless there are more than N jobs of the same name and it's not in the most recent N.
 *    Then reap it.
 * 4. A job not deployed, without history, and without a creation date should BE reaped. Only really
 *    old versions of Helios create jobs without dates.
 * 5. A job not deployed, without history, and with a creation date before the number of retention
 *    days should BE reaped.
 * 6. A job not deployed, without history, and with a creation date after the number of retention
 *    days should NOT BE reaped. Unless there are more than N jobs of the same name and it is not
 *    in the most recent N. Then reap it.
 *
 * Note that the --disable-job-history flag in {@link com.spotify.helios.agent.AgentParser} controls
 * whether the Helios agent should write job history to the data store. If this is disabled,
 * scenarios two and three above will never match. In this case, a job created a long time ago but
 * deployed recently may be reaped once it's undeployed even if the user needs it again in the
 * future.
 */
public class OldJobReaper extends RateLimitedService<Job> {

  private static final double PERMITS_PER_SECOND = 0.2; // one permit every 5 seconds
  private static final Clock SYSTEM_CLOCK = new SystemClock();
  private static final int DELAY = 60 * 24; // 1 day in minutes
  private static final TimeUnit TIME_UNIT = TimeUnit.MINUTES;
  private static final DateTimeFormatter DATE_FORMATTER =
      DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss");

  private static final Logger log = LoggerFactory.getLogger(OldJobReaper.class);

  private final MasterModel masterModel;
  private final long retentionDays;
  private final long retentionMillis;
  private final int numJobsToRetain;
  private final Clock clock;

  public OldJobReaper(final MasterModel masterModel,
                      final long retentionDays,
                      final int numJobsToRetain) {
    this(masterModel, retentionDays, numJobsToRetain, SYSTEM_CLOCK, PERMITS_PER_SECOND,
         new Random().nextInt(DELAY));
  }

  @VisibleForTesting
  OldJobReaper(final MasterModel masterModel,
               final long retentionDays,
               final int numJobsToRetain,
               final Clock clock,
               final double permitsPerSecond,
               final int initialDelay) {
    super(permitsPerSecond, initialDelay, DELAY, TIME_UNIT);
    this.masterModel = masterModel;
    this.retentionDays = retentionDays;
    this.retentionMillis = TimeUnit.DAYS.toMillis(retentionDays);
    this.numJobsToRetain = numJobsToRetain;
    this.clock = clock;
  }

  @Override
  Iterable<Job> collectItems() {
    return masterModel.getJobs().values();
  }

  @Override
  void processItem(final Job job) {
    log.info("Deciding whether to reap job.");
    final JobId jobId = job.getId();

    try {
      final JobStatus jobStatus = masterModel.getJobStatus(jobId);
      final Map<String, Deployment> deployments = jobStatus.getDeployments();
      final List<TaskStatusEvent> events = masterModel.getJobHistory(jobId);

      boolean reap;

      // If the number of jobs of the same name to be retained is specified,
      // check if this job is older than the most recent N jobs.
      final boolean olderThanNumJobsToRetain = olderThanNumJobsToRetain(
          job, numJobsToRetain, masterModel);

      if (deployments.isEmpty()) {
        if (events.isEmpty()) {
          final Long created = job.getCreated();
          if (created == null) {
            log.info("Marked job '{}' for reaping (not deployed, no history, no creation date)",
                     jobId);
            reap = true;
          } else if ((clock.now().getMillis() - created) > retentionMillis) {
            log.info("Marked job '{}' for reaping (not deployed, no history, creation date "
                     + "of {} before retention time of {} days)",
                     jobId, DATE_FORMATTER.print(created), retentionDays);
            reap = true;
          } else {
            if (olderThanNumJobsToRetain) {
              log.info("Marked job '{}' for reaping (not deployed, no history, creation date of {} "
                       + "after retention time of {} days, older than specified number of jobs {} "
                       + "of the same name to retain)",
                       jobId, DATE_FORMATTER.print(created), retentionDays, numJobsToRetain);
              reap = true;
            } else {
              log.info("NOT reaping job '{}' (not deployed, no history, creation date of {} after "
                       + "retention time of {} days)",
                       jobId, DATE_FORMATTER.print(created), retentionDays);
              reap = false;
            }
          }
        } else {
          // Get the last event which is the most recent
          final TaskStatusEvent event = events.get(events.size() - 1);
          final String eventDate = DATE_FORMATTER.print(event.getTimestamp());
          // Calculate the amount of time in milliseconds that has elapsed since the last event
          final long unusedDurationMillis = clock.now().getMillis() - event.getTimestamp();

          // A job not deployed, with history, and last used too long ago should BE reaped
          // A job not deployed, with history, and last used recently should NOT BE reaped
          if (unusedDurationMillis > retentionMillis) {
            log.info("Marked job '{}' for reaping (not deployed, has history whose last event "
                     + "on {} was before the retention time of {} days)",
                     jobId, eventDate, retentionDays);
            reap = true;
          } else {
            if (olderThanNumJobsToRetain) {
              log.info("NOT reaping job '{}' (not deployed, has history whose last event "
                       + "on {} was after the retention time of {} days, older than specified "
                       + "number of jobs {} of the same name to retain)",
                       jobId, eventDate, retentionDays, numJobsToRetain);
              reap = true;
            } else {
              log.info("NOT reaping job '{}' (not deployed, has history whose last event "
                       + "on {} was after the retention time of {} days)",
                       jobId, eventDate, retentionDays);
              reap = false;
            }
          }
        }
      } else {
        // A job that's deployed should NOT BE reaped regardless of its history or creation date
        log.info("NOT reaping job '{}' (it is deployed)", jobId);
        reap = false;
      }

      if (reap) {
        try {
          masterModel.removeJob(jobId, job.getToken());
        } catch (Exception e) {
          log.warn("Failed to reap old job '{}'", jobId, e);
        }
      }
    } catch (Exception e) {
      log.warn("Failed to determine if job '{}' should be reaped", jobId, e);
    }
  }

  @VisibleForTesting
  static boolean olderThanNumJobsToRetain(final Job job,
                                          final int numJobsToRetain,
                                          final MasterModel masterModel) {
    if (numJobsToRetain < 1) {
      return false;
    }

    final Map<JobId, Job> allJobs = masterModel.getJobs();

    // Filter jobs
    int numYounger = 0; // The number of jobs of the same name that are more recent
    for (final Map.Entry<JobId, Job> entry : allJobs.entrySet()) {
      if (entry.getKey().getName().equals(job.getId().getName())) {
        if (entry.getValue().getCreated() != null
            && job.getCreated() < entry.getValue().getCreated()) {
          numYounger++;
        }
      }
    }

    return numJobsToRetain <= numYounger;
  }
}
