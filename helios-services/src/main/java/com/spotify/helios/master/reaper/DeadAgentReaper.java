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
import com.spotify.helios.common.descriptors.AgentInfo;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.master.MasterModel;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * De-registers dead agents. An agent that has been DOWN for more than X hours is considered
 * dead.
 */
public class DeadAgentReaper extends RateLimitedService<String> {

  private static final double PERMITS_PER_SECOND = 0.2; // one permit every 5 seconds
  private static final Clock SYSTEM_CLOCK = new SystemClock();
  private static final int DELAY = 30;
  private static final TimeUnit TIME_UNIT = TimeUnit.MINUTES;

  private static final Logger log = LoggerFactory.getLogger(DeadAgentReaper.class);

  private final MasterModel masterModel;
  private final long timeoutMillis;
  private final Clock clock;

  public DeadAgentReaper(final MasterModel masterModel,
                         final long timeoutHours) {
    this(masterModel, timeoutHours, SYSTEM_CLOCK, PERMITS_PER_SECOND, new Random().nextInt(DELAY));
  }

  @VisibleForTesting
  DeadAgentReaper(final MasterModel masterModel,
                  final long timeoutHours,
                  final Clock clock,
                  final double permitsPerSecond,
                  final int initialDelay) {
    super(permitsPerSecond, initialDelay, DELAY, TIME_UNIT);
    this.masterModel = masterModel;
    checkArgument(timeoutHours > 0);
    this.timeoutMillis = TimeUnit.HOURS.toMillis(timeoutHours);
    this.clock = clock;
  }

  @Override
  Iterable<String> collectItems() {
    return masterModel.listHosts();
  }

  @Override
  void processItem(final String agent) {
    try {
      final HostStatus hostStatus = masterModel.getHostStatus(agent);
      if (hostStatus == null || hostStatus.getStatus() != HostStatus.Status.DOWN) {
        // Host not found or host not DOWN -- nothing to do
        return;
      }

      final AgentInfo agentInfo = hostStatus.getAgentInfo();
      if (agentInfo == null) {
        return;
      }

      final long downSince = agentInfo.getStartTime() + agentInfo.getUptime();
      final long downDurationMillis = clock.now().getMillis() - downSince;

      if (downDurationMillis >= timeoutMillis) {
        try {
          log.info("Reaping dead agent '{}' (DOWN for {} hours)",
                   agent, DurationFormatUtils.formatDurationHMS(downDurationMillis));
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
