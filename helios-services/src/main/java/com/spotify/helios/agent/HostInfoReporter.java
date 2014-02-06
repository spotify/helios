/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.AbstractScheduledService;

import com.spotify.helios.common.descriptors.HostInfo;
import com.spotify.helios.servicescommon.NodeUpdaterFactory;
import com.spotify.helios.servicescommon.ZooKeeperNodeUpdater;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.sun.management.OperatingSystemMXBean;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static java.util.concurrent.TimeUnit.MINUTES;

public class HostInfoReporter extends AbstractScheduledService {

  public static final int DEFAULT_INTERVAL = 1;
  public static final TimeUnit DEFAUL_TIMEUNIT = MINUTES;

  private final OperatingSystemMXBean operatingSystemMXBean;
  private final ZooKeeperNodeUpdater nodeUpdater;
  private final int interval;
  private final TimeUnit timeUnit;

  HostInfoReporter(final Builder builder) {
    this.operatingSystemMXBean = checkNotNull(builder.operatingSystemMXBean);
    this.nodeUpdater = builder.nodeUpdaterFactory.create(Paths.statusAgentHostInfo(builder.agent));
    this.interval = builder.interval;
    this.timeUnit = checkNotNull(builder.timeUnit);
  }

  @Override
  protected void runOneIteration() throws Exception {
      final String hostname = exec("uname -n").trim();
      final String uname = exec("uname -a").trim();

      final HostInfo hostInfo = HostInfo.newBuilder()
          .setArchitecture(operatingSystemMXBean.getArch())
          .setCpus(Runtime.getRuntime().availableProcessors())
          .setHostname(hostname)
          .setLoadAvg(operatingSystemMXBean.getSystemLoadAverage())
          .setOsName(operatingSystemMXBean.getName())
          .setOsVersion(operatingSystemMXBean.getVersion())
          .setMemoryFreeBytes(operatingSystemMXBean.getFreePhysicalMemorySize())
          .setMemoryTotalBytes(operatingSystemMXBean.getTotalPhysicalMemorySize())
          .setSwapFreeBytes(operatingSystemMXBean.getFreeSwapSpaceSize())
          .setSwapTotalBytes(operatingSystemMXBean.getTotalSwapSpaceSize())
          .setUname(uname)
          .build();

      nodeUpdater.update(hostInfo.toJsonBytes());
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(0, interval, timeUnit);
  }

  private String exec(final String command) {
    try {
      final Process process = Runtime.getRuntime().exec(command);
      return CharStreams.toString(new InputStreamReader(process.getInputStream()));
    } catch (IOException e) {
      throw propagate(e);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    Builder() {
    }

    private NodeUpdaterFactory nodeUpdaterFactory;
    private OperatingSystemMXBean operatingSystemMXBean;
    private String agent;
    private int interval = DEFAULT_INTERVAL;
    private TimeUnit timeUnit = DEFAUL_TIMEUNIT;

    public Builder setNodeUpdaterFactory(final NodeUpdaterFactory nodeUpdaterFactory) {
      this.nodeUpdaterFactory = nodeUpdaterFactory;
      return this;
    }

    public Builder setOperatingSystemMXBean(
        final OperatingSystemMXBean operatingSystemMXBean) {
      this.operatingSystemMXBean = operatingSystemMXBean;
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

    public HostInfoReporter build() {
      return new HostInfoReporter(this);
    }
  }
}
