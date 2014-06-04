package com.spotify.helios.agent;

import com.spotify.docker.messages.ContainerConfig;
import com.spotify.docker.messages.HostConfig;
import com.spotify.docker.messages.ImageInfo;
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
