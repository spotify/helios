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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.LogStream;
import com.spotify.helios.common.descriptors.ExecHealthCheck;
import com.spotify.helios.common.descriptors.HealthCheck;
import com.spotify.helios.common.descriptors.HttpHealthCheck;
import com.spotify.helios.common.descriptors.TcpHealthCheck;
import com.spotify.helios.servicescommon.DockerHost;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;

public class HealthCheckerFactory {

  public static HealthChecker create(final TaskConfig taskConfig, final DockerClient docker,
                                     final DockerHost dockerHost) {
    final HealthCheck healthCheck = taskConfig.healthCheck();

    if (healthCheck == null) {
      return null;
    } else if (healthCheck instanceof ExecHealthCheck) {
      return new ExecHealthChecker((ExecHealthCheck) healthCheck, docker);
    } else if (healthCheck instanceof HttpHealthCheck) {
      return new HttpHealthChecker((HttpHealthCheck) healthCheck, taskConfig, dockerHost);
    } else if (healthCheck instanceof TcpHealthCheck) {
      return new TcpHealthChecker((TcpHealthCheck) healthCheck, taskConfig, docker, dockerHost);
    }

    throw new IllegalArgumentException("Unknown healthCheck type");
  }

  static class ExecHealthChecker implements HealthChecker {

    private static final Logger log = LoggerFactory.getLogger(ExecHealthChecker.class);

    private final ExecHealthCheck healthCheck;
    private final DockerClient docker;

    ExecHealthChecker(final ExecHealthCheck healthCheck, final DockerClient docker) {
      this.healthCheck = healthCheck;
      this.docker = docker;
    }

    @Override
    public boolean check(final String containerId) {
      // Make sure we are on a docker version that supports exec health checks
      if (!compatibleDockerVersion(docker)) {
        throw new UnsupportedOperationException(
            "docker exec healthcheck is not supported on your docker version");
      }

      try {
        final List<String> cmd = healthCheck.getCommand();
        final String execId = docker.execCreate(containerId, cmd.toArray(new String[cmd.size()]),
                                                DockerClient.ExecParameter.STDOUT,
                                                DockerClient.ExecParameter.STDERR);

        final String output;
        try (LogStream stream = docker.execStart(execId)) {
          output = stream.readFully();
        }

        final int exitCode = docker.execInspect(execId).exitCode();
        if (exitCode != 0) {
          log.info("healthcheck failed with exit code {}. output {}", exitCode, output);
          return false;
        }

        return true;
      } catch (DockerException e) {
        return false;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }

    private static boolean compatibleDockerVersion(final DockerClient docker) {
      final String executionDriver;
      final String apiVersion;
      try {
        executionDriver = docker.info().executionDriver();
        apiVersion = docker.version().apiVersion();
      } catch (DockerException e) {
        return false;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }

      if (Strings.isNullOrEmpty(executionDriver) || !executionDriver.startsWith("native")) {
        return false;
      }
      if (Strings.isNullOrEmpty(apiVersion)) {
        return false;
      }

      final Iterable<String> split = Splitter.on(".").split(apiVersion);
      final int major = Integer.parseInt(Iterables.get(split, 0, "0"));
      final int minor = Integer.parseInt(Iterables.get(split, 1, "0"));
      return major == 1 && minor >= 18;
    }
  }

  private static class HttpHealthChecker implements HealthChecker {

    private static final int CONNECT_TIMEOUT_MILLIS = 500;
    private static final long READ_TIMEOUT_MILLIS = SECONDS.toMillis(10);

    private final HttpHealthCheck healthCheck;
    private final TaskConfig taskConfig;
    private final DockerHost dockerHost;

    private HttpHealthChecker(final HttpHealthCheck healthCheck, final TaskConfig taskConfig,
                              final DockerHost dockerHost) {
      this.healthCheck = healthCheck;
      this.taskConfig = taskConfig;
      this.dockerHost = dockerHost;
    }

    @Override
    public boolean check(final String containerId) throws InterruptedException, DockerException {
      final Integer port = taskConfig.ports().get(healthCheck.getPort()).getExternalPort();

      try {
        final URL url = new URL("http", dockerHost.address(), port, healthCheck.getPath());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(CONNECT_TIMEOUT_MILLIS);
        conn.setReadTimeout((int) READ_TIMEOUT_MILLIS);

        final int response = conn.getResponseCode();
        return response >= 200 && response <= 399;
      } catch (Exception e) {
        return false;
      }
    }
  }

  private static class TcpHealthChecker implements HealthChecker {

    private static final int CONNECT_TIMEOUT_MILLIS = 500;

    private final TcpHealthCheck healthCheck;
    private final TaskConfig taskConfig;
    private final DockerClient docker;
    private final DockerHost dockerHost;


    private TcpHealthChecker(final TcpHealthCheck healthCheck, final TaskConfig taskConfig,
                             final DockerClient docker, final DockerHost dockerHost) {
      this.healthCheck = healthCheck;
      this.taskConfig = taskConfig;
      this.docker = docker;
      this.dockerHost = dockerHost;
    }

    @Override
    public boolean check(final String containerId) throws InterruptedException, DockerException {
      final Integer port = taskConfig.ports().get(healthCheck.getPort()).getExternalPort();

      InetSocketAddress address = new InetSocketAddress(dockerHost.address(), port);
      if (address.getAddress().isLoopbackAddress()) {
        // tcp connections to a container-mapped port on loopback always succeed,
        // regardless of if the container is listening or not. use the bridge address instead.
        final String bridge = docker.inspectContainer(containerId).networkSettings().gateway();
        address = new InetSocketAddress(bridge, port);
      }

      try (final Socket s = new Socket()) {
        s.connect(address, CONNECT_TIMEOUT_MILLIS);
      } catch (Exception e) {
        return false;
      }

      return true;
    }
  }
}
