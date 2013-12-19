package com.spotify.helios.agent;

import com.kpelykh.docker.client.model.ContainerConfig;
import com.kpelykh.docker.client.model.HostConfig;
import com.kpelykh.docker.client.model.ImageInspectResponse;
import com.spotify.helios.common.descriptors.Job;

public class NoOpCommandWrapper implements CommandWrapper {
  @Override
  public void modifyStartConfig(HostConfig hostConfig) {
    //noop
  }

  @Override
  public void modifyCreateConfig(String image, Job job, ImageInspectResponse imageInfo,
      ContainerConfig containerConfig) {
    //noop
  }
}
