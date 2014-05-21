package com.spotify.helios.agent;

import com.spotify.helios.agent.docker.messages.ContainerConfig;
import com.spotify.helios.agent.docker.messages.HostConfig;
import com.spotify.helios.agent.docker.messages.ImageInfo;
import com.spotify.helios.common.descriptors.Job;

public interface CommandWrapper {
  void modifyStartConfig(HostConfig hostConfig);

  void modifyCreateConfig(String image, Job job, ImageInfo imageInfo,
                          ContainerConfig createConfig);
}
