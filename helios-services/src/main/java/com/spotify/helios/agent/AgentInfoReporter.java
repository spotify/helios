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

package com.spotify.helios.agent;

import com.spotify.helios.common.Version;
import com.spotify.helios.common.descriptors.AgentInfo;
import com.spotify.helios.servicescommon.coordination.NodeUpdaterFactory;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperNodeUpdater;

import java.lang.management.RuntimeMXBean;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Report various Agent runtime information via ZK so it can be visible to clients of Helios.
 */
public class AgentInfoReporter extends InterruptingScheduledService {

  public static final int DEFAULT_INTERVAL = 1;
  public static final TimeUnit DEFAUL_TIMEUNIT = MINUTES;

  private final RuntimeMXBean runtimeMXBean;
  private final ZooKeeperNodeUpdater nodeUpdater;
  private final int interval;
  private final TimeUnit timeUnit;

  AgentInfoReporter(final Builder builder) {
    this.runtimeMXBean = checkNotNull(builder.runtimeMXBean);
    this.nodeUpdater = builder.nodeUpdaterFactory.create(Paths.statusHostAgentInfo(builder.host));
    this.interval = builder.interval;
    this.timeUnit = checkNotNull(builder.timeUnit);
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

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    Builder() {
    }

    private NodeUpdaterFactory nodeUpdaterFactory;
    private RuntimeMXBean runtimeMXBean;
    private String host;
    private int interval = DEFAULT_INTERVAL;
    private TimeUnit timeUnit = DEFAUL_TIMEUNIT;

    public Builder setNodeUpdaterFactory(final NodeUpdaterFactory nodeUpdaterFactory) {
      this.nodeUpdaterFactory = nodeUpdaterFactory;
      return this;
    }

    public Builder setRuntimeMXBean(
        final RuntimeMXBean runtimeMXBean) {
      this.runtimeMXBean = runtimeMXBean;
      return this;
    }

    public Builder setHost(final String host) {
      this.host = host;
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

    public AgentInfoReporter build() {
      return new AgentInfoReporter(this);
    }
  }
}
