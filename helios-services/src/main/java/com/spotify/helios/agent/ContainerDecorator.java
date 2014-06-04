package com.spotify.helios.agent;

import com.spotify.helios.agent.docker.messages.ContainerConfig;
import com.spotify.helios.agent.docker.messages.HostConfig;
import com.spotify.helios.agent.docker.messages.ImageInfo;
import com.spotify.helios.common.descriptors.Job;

public interface ContainerDecorator {

  void decorateHostConfig(HostConfig.Builder hostConfig);

  void decorateContainerConfig(Job job, ImageInfo imageInfo,
                               ContainerConfig.Builder containerConfig);
}
