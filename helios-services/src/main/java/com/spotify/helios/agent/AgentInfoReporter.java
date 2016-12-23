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

import static java.util.Objects.requireNonNull;

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

/**
 * Report various Agent runtime information via ZK so it can be visible to clients of Helios.
 */
public class AgentInfoReporter extends SignalAwaitingService {


  private final RuntimeMXBean runtimeMxBean;
  private final ZooKeeperNodeUpdater nodeUpdater;
  private final int interval;
  private final TimeUnit timeUnit;

  AgentInfoReporter(RuntimeMXBean runtimeMxBean, NodeUpdaterFactory nodeUpdaterFactory, String host,
                    int interval, TimeUnit timeUnit, CountDownLatch latch) {
    super(latch);
    this.runtimeMxBean = requireNonNull(runtimeMxBean);
    this.nodeUpdater = nodeUpdaterFactory.create(Paths.statusHostAgentInfo(host));
    this.interval = interval;
    this.timeUnit = requireNonNull(timeUnit);
  }

  @Override
  protected void runOneIteration() {
    final AgentInfo agentInfo = AgentInfo.newBuilder()
        .setName(runtimeMxBean.getName())
        .setVmName(runtimeMxBean.getVmName())
        .setVmVendor(runtimeMxBean.getVmVendor())
        .setVmVersion(runtimeMxBean.getVmVersion())
        .setSpecName(runtimeMxBean.getSpecName())
        .setSpecVendor(runtimeMxBean.getSpecVendor())
        .setSpecVersion(runtimeMxBean.getSpecVersion())
        .setInputArguments(runtimeMxBean.getInputArguments())
        .setUptime(runtimeMxBean.getUptime())
        .setStartTime(runtimeMxBean.getStartTime())
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
