/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios.agent;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.DockerHost;
import com.spotify.docker.client.LogStream;
import com.spotify.helios.common.descriptors.ExecHealthCheck;
import com.spotify.helios.common.descriptors.HealthCheck;
import com.spotify.helios.common.descriptors.HttpHealthCheck;
import com.spotify.helios.common.descriptors.TcpHealthCheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class HealthCheckerFactory {

  private HealthCheckerFactory() {
  }

  public static HealthChecker create(final TaskConfig taskConfig, final DockerClient docker,
                                     final DockerHost dockerHost) {
    final HealthCheck healthCheck = taskConfig.healthCheck();

    if (healthCheck == null) {
      return null;
    } else if (healthCheck instanceof ExecHealthCheck) {
      return new ExecHealthChecker((ExecHealthCheck) healthCheck, docker);
    } else if (healthCheck instanceof HttpHealthCheck) {
      return new HttpHealthChecker((HttpHealthCheck) healthCheck, taskConfig, docker, dockerHost);
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
                                                DockerClient.ExecCreateParam.attachStdout(),
                                                DockerClient.ExecCreateParam.attachStderr());

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

  private abstract static class NetworkHealthchecker implements HealthChecker {
    private final DockerClient dockerClient;

    protected NetworkHealthchecker(final DockerClient dockerClient) {
      this.dockerClient = dockerClient;
    }

    protected String getBridgeAddress(String containerId)
        throws DockerException, InterruptedException {
      return dockerClient.inspectContainer(containerId).networkSettings().gateway();
    }
  }

  private static class HttpHealthChecker extends NetworkHealthchecker {

    private static final Logger log = LoggerFactory.getLogger(HttpHealthChecker.class);


    private static final int CONNECT_TIMEOUT_MILLIS = 500;
    private static final long READ_TIMEOUT_MILLIS = SECONDS.toMillis(10);

    private final HttpHealthCheck healthCheck;
    private final TaskConfig taskConfig;
    private final DockerHost dockerHost;

    private HttpHealthChecker(final HttpHealthCheck healthCheck, final TaskConfig taskConfig,
                              final DockerClient dockerClient, final DockerHost dockerHost) {
      super(dockerClient);
      this.healthCheck = healthCheck;
      this.taskConfig = taskConfig;
      this.dockerHost = dockerHost;
    }

    @Override
    public boolean check(final String containerId) throws InterruptedException, DockerException {

      final String host;
      if (dockerHost.host().startsWith("unix://")) {
        host = getBridgeAddress(containerId);
        log.info("Using bridge address {} for healthchecks", host);
      } else {
        host = dockerHost.address();
      }

      final URL url;
      // TODO (mbrown): is port always non-null? it is unconditionally unboxed on the next line
      final Integer port = taskConfig.ports().get(healthCheck.getPort()).getExternalPort();
      try {
        url = new URL("http", host, port, healthCheck.getPath());
      } catch (MalformedURLException e1) {
        throw Throwables.propagate(e1);
      }

      log.info("about to healthcheck containerId={} with url={} for task={}",
               containerId, url, taskConfig);

      try {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(CONNECT_TIMEOUT_MILLIS);
        conn.setReadTimeout((int) READ_TIMEOUT_MILLIS);

        final int response = conn.getResponseCode();
        log.warn("healthcheck for containerId={} with url={} returned status={}",
                 containerId, url, response);
        return response >= 200 && response <= 399;
      } catch (Exception e) {
        log.warn("exception in healthchecking containerId={} with url={}", containerId, url, e);
        return false;
      }
    }

  }

  private static class TcpHealthChecker extends NetworkHealthchecker {

    private static final Logger log = LoggerFactory.getLogger(TcpHealthChecker.class);

    private static final int CONNECT_TIMEOUT_MILLIS = 500;

    private final TcpHealthCheck healthCheck;
    private final TaskConfig taskConfig;
    private final DockerHost dockerHost;


    private TcpHealthChecker(final TcpHealthCheck healthCheck, final TaskConfig taskConfig,
                             final DockerClient docker, final DockerHost dockerHost) {
      super(docker);
      this.healthCheck = healthCheck;
      this.taskConfig = taskConfig;
      this.dockerHost = dockerHost;
    }

    @Override
    public boolean check(final String containerId) throws InterruptedException, DockerException {
      final Integer port = taskConfig.ports().get(healthCheck.getPort()).getExternalPort();

      InetSocketAddress address = new InetSocketAddress(dockerHost.address(), port);
      if (address.getAddress().isLoopbackAddress()) {
        // tcp connections to a container-mapped port on loopback always succeed,
        // regardless of if the container is listening or not. use the bridge address instead.
        address = new InetSocketAddress(getBridgeAddress(containerId), port);
      }

      log.info("about to healthcheck containerId={} with address={} for task={}",
               containerId, address, taskConfig);

      try (final Socket s = new Socket()) {
        s.connect(address, CONNECT_TIMEOUT_MILLIS);
      } catch (Exception e) {
        return false;
      }

      return true;
    }
  }
}
