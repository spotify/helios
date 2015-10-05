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

import com.google.common.io.CharStreams;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.helios.common.descriptors.DockerVersion;
import com.spotify.helios.common.descriptors.HostInfo;
import com.spotify.helios.servicescommon.DockerHost;
import com.spotify.helios.servicescommon.coordination.NodeUpdaterFactory;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperNodeUpdater;
import com.sun.management.OperatingSystemMXBean;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Reports various bits of system information to ZK so it can be viewed via the the API.
 */
public class HostInfoReporter extends InterruptingScheduledService {

  public static final int DEFAULT_INTERVAL = 1;
  public static final TimeUnit DEFAUL_TIMEUNIT = MINUTES;

  private final OperatingSystemMXBean operatingSystemMXBean;
  private final ZooKeeperNodeUpdater nodeUpdater;
  private final int interval;
  private final TimeUnit timeUnit;
  private final DockerClient dockerClient;
  private final DockerHost dockerHost;

  HostInfoReporter(final Builder builder) {
    this.operatingSystemMXBean = checkNotNull(builder.operatingSystemMXBean,
                                              "operatingSystemMXBean");
    this.nodeUpdater = builder.nodeUpdaterFactory.create(
        Paths.statusHostInfo(checkNotNull(builder.host, "host")));
    this.dockerClient = checkNotNull(builder.dockerClient, "dockerClient");
    this.dockerHost = checkNotNull(builder.dockerHost, "dockerHost");
    this.interval = builder.interval;
    this.timeUnit = checkNotNull(builder.timeUnit, "timeUnit");
  }

  @Override
  protected void runOneIteration() throws InterruptedException {
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
        .setDockerVersion(dockerVersion())
        .setDockerHost(dockerHost())
        .setDockerCertPath(dockerHost.dockerCertPath())
        .build();

    nodeUpdater.update(hostInfo.toJsonBytes());
  }

  private DockerVersion dockerVersion() throws InterruptedException {
    try {
      final com.spotify.docker.client.messages.Version version = dockerClient.version();
      return version == null ? null : dockerVersion(version);
    } catch (DockerException e) {
      return null;
    }
  }

  private DockerVersion dockerVersion(final com.spotify.docker.client.messages.Version version) {
    return DockerVersion.builder()
        .apiVersion(version.apiVersion())
        .arch(version.arch())
        .gitCommit(version.gitCommit())
        .goVersion(version.goVersion())
        .kernelVersion(version.kernelVersion())
        .os(version.os())
        .version(version.version())
        .build();
  }

  private String dockerHost() {
    final String host = dockerHost.host();
    if (host.startsWith("unix://")) {
      return host;
    } else {
      return "tcp://" + host;
    }
  }

  @Override
  protected ScheduledFuture<?> schedule(final Runnable runnable,
                                        final ScheduledExecutorService executorService) {
    return executorService.scheduleWithFixedDelay(runnable, 0, interval, timeUnit);
  }

  private String exec(final String command) {
    try {
      final Process process = Runtime.getRuntime().exec(command);
      return CharStreams.toString(new InputStreamReader(process.getInputStream(), UTF_8));
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
    private String host;
    private DockerClient dockerClient;
    private DockerHost dockerHost;
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

    public Builder setHost(final String host) {
      this.host = host;
      return this;
    }

    public Builder setDockerClient(final DockerClient dockerClient) {
      this.dockerClient = dockerClient;
      return this;
    }

    public Builder setDockerHost(final DockerHost dockerHost) {
      this.dockerHost = dockerHost;
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
