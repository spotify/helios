package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;

import java.util.Map;

public interface StatusUpdater {
  void setStatus(final TaskStatus.State status);
  void setStatus(final TaskStatus.State status, final String containerId);
  void setStatus(final TaskStatus.State status, final String containerId,
                 final Map<String, PortMapping> ports);
}
