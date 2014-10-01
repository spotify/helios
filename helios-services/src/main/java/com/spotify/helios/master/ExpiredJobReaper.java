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

package com.spotify.helios.master;

import com.google.common.collect.ImmutableList;
import com.spotify.helios.agent.InterruptingScheduledService;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The ExpiredJobReaper periodically checks if any jobs in the cluster have expired.
 * For any job that is at or past its expiration date, it undeploys the job from
 * any deployed hosts, and then removes the job entirely from the cluster.
 */
public class ExpiredJobReaper extends InterruptingScheduledService {

  private static final Logger log = LoggerFactory.getLogger(ExpiredJobReaper.class);

  public static final int DEFAULT_INTERVAL = 30;
  public static final TimeUnit DEFAUL_TIMEUNIT = SECONDS;

  private final MasterModel masterModel;
  private final int interval;
  private final TimeUnit timeUnit;

  private ExpiredJobReaper(final Builder builder) {
    this.masterModel = builder.masterModel;
    this.interval = builder.interval;
    this.timeUnit = checkNotNull(builder.timeUnit);
  }

  @Override
  protected void runOneIteration() {
    for (Entry<JobId, Job> entry : masterModel.getJobs().entrySet()) {
      final JobId jobId = entry.getKey();
      final Job job = entry.getValue();

      if (job.getExpires() == null) {
        continue;
      }
      if (job.getExpires().getTime() <= System.currentTimeMillis()) {
        final JobStatus status = masterModel.getJobStatus(jobId);
        final List<String> hosts = ImmutableList.copyOf(status.getDeployments().keySet());

        for (String host : hosts) {
          try {
            masterModel.undeployJob(host, jobId);
          } catch (HostNotFoundException e) {
            log.error("couldn't undeploy job {} from host {} when it hit deadline", jobId, host, e);
          } catch (JobNotDeployedException e) {
            log.debug("job {} was already undeployed when it hit deadline", jobId, e);
          }
        }

        try {
          masterModel.removeJob(jobId);
        } catch (JobDoesNotExistException e) {
          log.debug("job {} was already removed when it hit deadline", jobId, e);
        } catch (JobStillDeployedException e) {
          log.debug("job {} still deployed on some host(s) after expiry reap", jobId, e);
        }
      }
    }
  }

  @Override
  protected ScheduledFuture<?> schedule(final Runnable runnable,
                                        final ScheduledExecutorService executorService) {
    return executorService.scheduleWithFixedDelay(runnable, 0, interval, timeUnit);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private Builder() {
    }

    private MasterModel masterModel;
    private int interval = DEFAULT_INTERVAL;
    private TimeUnit timeUnit = DEFAUL_TIMEUNIT;

    public Builder setMasterModel(final MasterModel masterModel) {
      this.masterModel = masterModel;
      return this;
    }

    public Builder setInterval(final int interval) {
      this.interval = interval;
      return this;
    }

    public Builder setTimeUnit(final TimeUnit timeUnit) {
      this.timeUnit = timeUnit;
      return this;
    }

    public ExpiredJobReaper build() {
      return new ExpiredJobReaper(this);
    }
  }
}
