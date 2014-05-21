package com.spotify.helios.agent;

import com.spotify.helios.agent.docker.messages.ContainerConfig;
import com.spotify.helios.agent.docker.messages.HostConfig;
import com.spotify.helios.agent.docker.messages.ImageInfo;
import com.spotify.helios.common.descriptors.Job;

public class NoOpCommandWrapper implements CommandWrapper {
  @Override
  public void modifyStartConfig(HostConfig hostConfig) {
    //noop
  }

  @Override
  public void modifyCreateConfig(String image, Job job, ImageInfo imageInfo,
      ContainerConfig containerConfig) {
    //noop
  }
}
