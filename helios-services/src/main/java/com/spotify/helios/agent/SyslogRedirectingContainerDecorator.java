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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.ImageInfo;
import com.spotify.docker.client.messages.LogConfig;
import com.spotify.helios.common.descriptors.Job;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bind mounts /usr/lib/helios inside the container as /helios, and uses the syslog-redirector
 * executable there to redirect container stdout/err to syslog.
 */
public class SyslogRedirectingContainerDecorator implements ContainerDecorator {

  private final String syslogHostPort;

  public SyslogRedirectingContainerDecorator(final String syslogHostPort) {
    this.syslogHostPort = syslogHostPort;
  }

  @Override
  public void decorateHostConfig(Job job, Optional<String> dockerVersion,
                                 HostConfig.Builder hostConfigBuilder) {
    final HostConfig hostConfig = hostConfigBuilder.build();
    if (useSyslogRedirector(dockerVersion)) {
      final List<String> binds = Lists.newArrayList();
      if (hostConfig.binds() != null) {
        binds.addAll(hostConfig.binds());
      }
      binds.add("/usr/lib/helios:/helios:ro");
      hostConfigBuilder.binds(binds);
    } else {
      final ImmutableMap.Builder<String, String> logOpts = ImmutableMap.builder();

      logOpts.put("syslog-address", "udp://" + syslogHostPort);
      logOpts.put("syslog-facility", "local0"); // match the behavior of syslog-redirector

      logOpts.put("tag", job.getId().toString());

      hostConfigBuilder.logConfig(LogConfig.create("syslog", logOpts.build()));
    }
  }

  @Override
  public void decorateContainerConfig(Job job, ImageInfo imageInfo, Optional<String> dockerVersion,
                                      ContainerConfig.Builder containerConfigBuilder) {
    if (!useSyslogRedirector(dockerVersion)) {
      return;
    }

    final ContainerConfig imageConfig = imageInfo.config();

    // Inject syslog-redirector in the entrypoint to capture std out/err
    final String syslogRedirectorPath = Optional.fromNullable(job.getEnv().get("SYSLOG_REDIRECTOR"))
        .or("/helios/syslog-redirector");

    final List<String> entrypoint = Lists.newArrayList(syslogRedirectorPath,
        "-h", syslogHostPort,
        "-n", job.getId().toString(),
        "--");
    if (imageConfig.entrypoint() != null) {
      entrypoint.addAll(imageConfig.entrypoint());
    }
    containerConfigBuilder.entrypoint(entrypoint);

    final ContainerConfig containerConfig = containerConfigBuilder.build();

    // If there's no explicit container cmd specified, copy over the one from the image.
    // Only setting the entrypoint causes dockerd to not use the image cmd.
    if ((containerConfig.cmd() == null || containerConfig.cmd().isEmpty())
        && imageConfig.cmd() != null) {
      containerConfigBuilder.cmd(imageConfig.cmd());
    }

    final ImmutableMap.Builder<String, Map> volumesBuilder = ImmutableMap.builder();
    if (containerConfig.volumes() != null) {
      volumesBuilder.putAll(containerConfig.volumes());
    }
    volumesBuilder.put("/helios", new HashMap<>());
    containerConfigBuilder.volumes(volumesBuilder.build());
  }

  private boolean useSyslogRedirector(Optional<String> dockerVersion) {
    if (!dockerVersion.isPresent()) {
      // always use syslog-redirector if we can't figure out the version
      return true;
    } else {
      final String version = dockerVersion.get();

      if (version.startsWith("1.6.") || version.startsWith("1.7.") || version.startsWith("1.8.")) {
        // old version that doesn't have the builtin syslog configurability we need
        return true;
      }
    }

    return false;
  }
}
