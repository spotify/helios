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
import com.spotify.helios.common.descriptors.AgentInfo;
import com.spotify.helios.common.descriptors.HostStatus;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * De-registers dead agents, where an agent that has been DOWN for more than X hours is considered
 * dead.
 */
public class DeadAgentReaper extends InterruptingScheduledService {

  private static final Clock SYSTEM_CLOCK = new SystemClock();
  private static final long INTERVAL = 30;
  private static final TimeUnit INTERVAL_TIME_UNIT = TimeUnit.MINUTES;

  private static final Logger log = LoggerFactory.getLogger(DeadAgentReaper.class);

  private final MasterModel masterModel;
  private final long timeoutMillis;
  private final Clock clock;

  public DeadAgentReaper(final MasterModel masterModel,
                         final long timeoutHours) {
    this(masterModel, timeoutHours, SYSTEM_CLOCK);
  }

  public DeadAgentReaper(final MasterModel masterModel,
                         final long timeoutHours,
                         final Clock clock) {
    this.masterModel = masterModel;
    checkArgument(timeoutHours > 0);
    this.timeoutMillis = TimeUnit.HOURS.toMillis(timeoutHours);
    this.clock = clock;
  }

  @Override
  protected void runOneIteration() {
    log.debug("Reaping agents");
    final List<String> agents = masterModel.listHosts();
    for (final String agent : agents) {
      try {
        final HostStatus hostStatus = masterModel.getHostStatus(agent);
        if (hostStatus == null || hostStatus.getStatus() != HostStatus.Status.DOWN) {
          // Host not found or host not DOWN -- nothing to do, move on to the next host
          continue;
        }

        final AgentInfo agentInfo = hostStatus.getAgentInfo();
        if (agentInfo == null) {
          continue;
        }

        final long downSince = agentInfo.getStartTime() + agentInfo.getUptime();
        final long downDurationMillis = clock.now().getMillis() - downSince;

        if (downDurationMillis >= timeoutMillis) {
          try {
            log.info("Reaping dead agent '{}' (DOWN for {} hours)",
                     DurationFormatUtils.formatDurationHMS(downDurationMillis));
            masterModel.deregisterHost(agent);
          } catch (Exception e) {
            log.warn("Failed to reap agent '{}'", agent, e);
          }
        }
      } catch (Exception e) {
        log.warn("Failed to determine if agent '{}' should be reaped", agent, e);
      }
    }
  }

  @Override
  protected ScheduledFuture<?> schedule(final Runnable runnable,
                                        final ScheduledExecutorService executorService) {
    return executorService.scheduleWithFixedDelay(runnable, 0, INTERVAL, INTERVAL_TIME_UNIT);
  }
}
