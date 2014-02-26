/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.AgentInfo;
import com.spotify.helios.servicescommon.NodeUpdaterFactory;
import com.spotify.helios.servicescommon.ZooKeeperNodeUpdater;
import com.spotify.helios.servicescommon.coordination.Paths;

import java.lang.management.RuntimeMXBean;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MINUTES;

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
        .build();

    nodeUpdater.update(agentInfo.toJsonBytes());
  }

  @Override
  protected ScheduledFuture<?> schedule(Runnable runnable, ScheduledExecutorService executorService) {
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
