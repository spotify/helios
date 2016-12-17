/*-
 * -\-\-
 * Helios Services
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

package com.spotify.helios.agent;

import com.spotify.helios.common.Version;
import com.spotify.helios.common.descriptors.AgentInfo;
import com.spotify.helios.servicescommon.coordination.NodeUpdaterFactory;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperNodeUpdater;

import java.lang.management.RuntimeMXBean;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Report various Agent runtime information via ZK so it can be visible to clients of Helios.
 */
public class AgentInfoReporter extends SignalAwaitingService {


  private final RuntimeMXBean runtimeMXBean;
  private final ZooKeeperNodeUpdater nodeUpdater;
  private final int interval;
  private final TimeUnit timeUnit;

  AgentInfoReporter(RuntimeMXBean runtimeMXBean, NodeUpdaterFactory nodeUpdaterFactory, String host,
                    int interval, TimeUnit timeUnit, CountDownLatch latch) {
    super(latch);
    this.runtimeMXBean = requireNonNull(runtimeMXBean);
    this.nodeUpdater = nodeUpdaterFactory.create(Paths.statusHostAgentInfo(host));
    this.interval = interval;
    this.timeUnit = requireNonNull(timeUnit);
  }

  @Override
  protected void runOneIteration() {
    final AgentInfo agentInfo = AgentInfo.newBuilder()
        .setName(runtimeMXBean.getName())
        .setVmName(runtimeMXBean.getVmName())
        .setVmVendor(runtimeMXBean.getVmVendor())
        .setVmVersion(runtimeMXBean.getVmVersion())
        .setSpecName(runtimeMXBean.getSpecName())
        .setSpecVendor(runtimeMXBean.getSpecVendor())
        .setSpecVersion(runtimeMXBean.getSpecVersion())
        .setInputArguments(runtimeMXBean.getInputArguments())
        .setUptime(runtimeMXBean.getUptime())
        .setStartTime(runtimeMXBean.getStartTime())
        .setVersion(Version.POM_VERSION)
        .build();

    nodeUpdater.update(agentInfo.toJsonBytes());
  }

  @Override
  protected ScheduledFuture<?> schedule(final Runnable runnable,
                                        final ScheduledExecutorService executorService) {
    return executorService.scheduleWithFixedDelay(runnable, 0, interval, timeUnit);
  }
}
