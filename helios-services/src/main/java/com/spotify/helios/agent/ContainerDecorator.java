package com.spotify.helios.agent;

import com.spotify.docker.messages.ContainerConfig;
import com.spotify.docker.messages.HostConfig;
import com.spotify.docker.messages.ImageInfo;
import com.spotify.helios.common.descriptors.Job;

public interface ContainerDecorator {

  void decorateHostConfig(HostConfig.Builder hostConfig);

  void decorateContainerConfig(Job job, ImageInfo imageInfo,
                               ContainerConfig.Builder containerConfig);
}
