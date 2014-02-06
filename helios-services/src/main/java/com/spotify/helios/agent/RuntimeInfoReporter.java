/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.util.concurrent.AbstractScheduledService;

import com.spotify.helios.common.descriptors.RuntimeInfo;
import com.spotify.helios.servicescommon.NodeUpdaterFactory;
import com.spotify.helios.servicescommon.ZooKeeperNodeUpdater;
import com.spotify.helios.servicescommon.coordination.Paths;

import java.lang.management.RuntimeMXBean;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MINUTES;

public class RuntimeInfoReporter extends AbstractScheduledService {

  public static final int DEFAULT_INTERVAL = 1;
  public static final TimeUnit DEFAUL_TIMEUNIT = MINUTES;

  private final RuntimeMXBean runtimeMXBean;
  private final ZooKeeperNodeUpdater nodeUpdater;
  private final int interval;
  private final TimeUnit timeUnit;

  RuntimeInfoReporter(final Builder builder) {
    this.runtimeMXBean = checkNotNull(builder.runtimeMXBean);
    this.nodeUpdater = builder.nodeUpdaterFactory.create(
        Paths.statusAgentRuntimeInfo(builder.agent));
    this.interval = builder.interval;
    this.timeUnit = checkNotNull(builder.timeUnit);
  }

  @Override
  protected void runOneIteration() throws Exception {
    final RuntimeInfo runtimeInfo = RuntimeInfo.newBuilder()
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

    nodeUpdater.update(runtimeInfo.toJsonBytes());
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(0, interval, timeUnit);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    Builder() {
    }

    private NodeUpdaterFactory nodeUpdaterFactory;
    private RuntimeMXBean runtimeMXBean;
    private String agent;
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

    public Builder setAgent(final String agent) {
      this.agent = agent;
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

    public RuntimeInfoReporter build() {
      return new RuntimeInfoReporter(this);
    }
  }
}
