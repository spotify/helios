package com.spotify.helios.agent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;

import com.kpelykh.docker.client.model.ContainerConfig;
import com.kpelykh.docker.client.model.HostConfig;
import com.kpelykh.docker.client.model.ImageInspectResponse;
import com.spotify.helios.common.descriptors.Job;

import java.util.List;
import java.util.Map;

/**
 * Bind mounts /usr/lib/helios inside the container as /helios, and uses the syslog-redirector
 * executable there to redirect container stdout/err to syslog.
 */
public class SyslogRedirectingCommandWrapper implements CommandWrapper {
  private String syslogHostPort;

  public SyslogRedirectingCommandWrapper(String syslogHostPort) {
    this.syslogHostPort = syslogHostPort;
  }

  @Override
  public void modifyStartConfig(HostConfig hostConfig) {
    hostConfig.binds = new String[]{"/usr/lib/helios:/helios:ro"};
  }

  @Override
  public void modifyCreateConfig(String image, Job job, ImageInspectResponse imageInfo,
      ContainerConfig containerConfig) {
    ContainerConfig imageConfig = imageInfo.containerConfig;
    if (imageConfig.getEntrypoint() != null) {
      containerConfig.setEntrypoint(wrap(containerConfig.getEntrypoint(), job));
    } else {
      containerConfig.setCmd(wrap(containerConfig.getCmd(), job));
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> volumes = (Map<String, Object>) containerConfig.getVolumes();
    final Builder<String, Object> builder = new ImmutableMap.Builder<String, Object>();
    if (volumes != null) {
      builder.putAll(volumes);
    }

    builder.put("/helios", ImmutableMap.<String, String>of());
    containerConfig.setVolumes(builder.build());
  }

  private String[] wrap(String[] command, Job job) {
    List<String> result = Lists.newArrayList(
        "/helios/syslog-redirector", "-h", syslogHostPort, "-n", job.getId().toString(), "--");
    result.addAll(ImmutableList.copyOf(command));
    return result.toArray(new String[]{});
  }
}
