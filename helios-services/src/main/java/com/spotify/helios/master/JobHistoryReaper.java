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
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Removes job histories whose corresponding jobs don't exist anymore.
 * There are two race conditions where jobs can be deleted but their histories are
 * left behind in ZooKeeper:
 *
 * 1. The master deletes the job (in {@link ZooKeeperMasterModel} and then deletes its history.
 * During this deletion, the agent creates a znode. The master's deletion operations fail.
 *
 * 2. The master deletes all relevant history znodes successfully. The agent still hasn't undeployed
 * its job and continues writing history to ZooKeeper. This will recreate deleted history znodes
 * via {@link com.spotify.helios.agent.TaskHistoryWriter}.
 *
 * Solve both of these cases by scheduling an instance of this class. It runs once a day once
 * scheduled.
 */
class JobHistoryReaper extends InterruptingScheduledService {

  private static final Logger log = LoggerFactory.getLogger(JobHistoryReaper.class);

  private final MasterModel masterModel;
  private final ZooKeeperClient client;

  JobHistoryReaper(final MasterModel masterModel,
                   final ZooKeeperClient client) {
    this.masterModel = masterModel;
    this.client = client;
  }

  @Override
  protected void runOneIteration() {
    log.debug("Reaping orphaned job histories.");

    final String path = Paths.historyJobs();
    List<String> jobIds = Collections.emptyList();

    try {
      jobIds = client.getChildren(path);
    } catch (KeeperException e) {
      log.warn("Failed to get children of znode {}", path, e);
    }

    for (final String jobId : jobIds) {
      final JobId id = JobId.fromString(jobId);
      final Job job = masterModel.getJob(id);
      if (job == null) {
        try {
          client.deleteRecursive(Paths.historyJob(id));
        } catch (NoNodeException ignored) {
          // Something deleted the history right before we got to it. Ignore and keep going.
        } catch (KeeperException e) {
          log.warn("error removing job history for job {}", jobId, e);
        }
      }
    }
  }

  @Override
  protected ScheduledFuture<?> schedule(final Runnable runnable,
                                        final ScheduledExecutorService executorService) {
    return executorService.scheduleWithFixedDelay(runnable, 0, 1, TimeUnit.DAYS);
  }
}
