package com.spotify.helios.agent;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.ImageInfo;
import com.spotify.helios.common.descriptors.Job;

import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * Bind mounts /usr/lib/helios inside the container as /helios, and uses the syslog-redirector
 * executable there to redirect container stdout/err to syslog.
 */
public class SyslogRedirectingContainerDecorator implements ContainerDecorator {

  private final String syslogHostPort;

  public SyslogRedirectingContainerDecorator(String syslogHostPort) {
    this.syslogHostPort = syslogHostPort;
  }

  @Override
  public void decorateHostConfig(HostConfig.Builder hostConfig) {
    hostConfig.binds(asList("/usr/lib/helios:/helios:ro"));
  }

  @Override
  public void decorateContainerConfig(Job job, ImageInfo imageInfo,
                                      ContainerConfig.Builder containerConfig) {
    ContainerConfig imageConfig = imageInfo.containerConfig();

    final List<String> entrypoint = Lists.newArrayList("/helios/syslog-redirector",
                                                       "-h", syslogHostPort,
                                                       "-n", job.getId().toString(),
                                                       "--");
    if (imageConfig.entrypoint() != null) {
      entrypoint.addAll(imageConfig.entrypoint());
    }
    containerConfig.entrypoint(entrypoint);

    final Set<String> volumes = Sets.newHashSet();
    if (containerConfig.volumes() != null) {
      volumes.addAll(containerConfig.volumes());
    }
    volumes.add("/helios");
    containerConfig.volumes(volumes);
  }
}
