package com.spotify.helios.agent;

import com.spotify.helios.agent.docker.messages.ContainerConfig;
import com.spotify.helios.agent.docker.messages.HostConfig;
import com.spotify.helios.agent.docker.messages.ImageInfo;
import com.spotify.helios.common.descriptors.Job;

public class NoOpContainerDecorator implements ContainerDecorator {

  @Override
  public void decorateHostConfig(HostConfig.Builder hostConfig) {
    //noop
  }

  @Override
  public void decorateContainerConfig(Job job, ImageInfo imageInfo,
                                      ContainerConfig.Builder containerConfig) {
    //noop
  }
}
