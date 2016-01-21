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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.spotify.helios.agent.InterruptingScheduledService;
import com.spotify.helios.common.Clock;
import com.spotify.helios.common.SystemClock;
import com.spotify.helios.common.descriptors.AgentInfo;
import com.spotify.helios.common.descriptors.HostStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * De-registers dead agents, where an agent that has been DOWN for more than X time is considered
 * dead.
 */
public class DeadAgentReaper extends InterruptingScheduledService {

  private static final Clock SYSTEM_CLOCK = new SystemClock();
  private static final long INTERVAL = 30;
  private static final TimeUnit INTERVAL_TIME_UNIT = TimeUnit.MINUTES;

  private static final Logger log = LoggerFactory.getLogger(DeadAgentReaper.class);

  private final MasterModel masterModel;
  private final long timeoutMillis;

  /**
   * We check for dead agents every 30 minutes, which means the actual timeout the specified timeout
   * plus up to 30 minutes. I.e. very low timeouts will not work in practice.
   */
  public DeadAgentReaper(final MasterModel masterModel,
                         final long timeout,
                         final TimeUnit timeUnit) {
    this.masterModel = masterModel;
    this.timeoutMillis = timeUnit.toMillis(timeout);
  }

  @Override
  protected void runOneIteration() {
    log.debug("Reaping agents");
    final List<String> deadAgents = getDeadAgents(masterModel, timeoutMillis, SYSTEM_CLOCK);
    log.debug("Reaping {} dead agents: {}", deadAgents.size(), deadAgents);

    for (final String agent : deadAgents) {
      log.info("Reaping dead agent {}", agent);
      try {
        masterModel.deregisterHost(agent);
      } catch (Exception e) {
        log.error("Failed to reap dead agent {}", agent, e);
      }
    }
  }

  @VisibleForTesting
  static List<String> getDeadAgents(final MasterModel masterModel, final long timeoutMillis,
                                    final Clock clock) {
    final List<String> deadAgents = Lists.newArrayList();
    final List<String> agents = masterModel.listHosts();

    for (final String agent : agents) {
      try {
        final HostStatus hostStatus = masterModel.getHostStatus(agent);
        if (hostStatus == null || hostStatus.getStatus() != HostStatus.Status.DOWN) {
          // Hot not found or host not DOWN -- nothing to do, move on to the next host
          continue;
        }

        final AgentInfo agentInfo = hostStatus.getAgentInfo();
        if (agentInfo == null) {
          continue;
        }

        final long downSince = agentInfo.getStartTime() + agentInfo.getUptime();
        final long downDurationMillis = clock.now().getMillis() - downSince;

        if (downDurationMillis >= timeoutMillis) {
          deadAgents.add(agent);
        }
      } catch (Exception e) {
        log.warn("Failed to determine if agent '{}' should be reaped", agent, e);
      }
    }

    return deadAgents;
  }

  @Override
  protected ScheduledFuture<?> schedule(final Runnable runnable,
                                        final ScheduledExecutorService executorService) {
    return executorService.scheduleWithFixedDelay(runnable, 0, INTERVAL, INTERVAL_TIME_UNIT);
  }
}
