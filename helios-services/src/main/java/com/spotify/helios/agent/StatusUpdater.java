package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.TaskStatus;

public interface StatusUpdater {
  void setStatus(final TaskStatus.State status);
  void setStatus(final TaskStatus.State status, final String containerId);
}
