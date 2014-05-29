package com.spotify.helios.agent;

import com.google.common.base.Supplier;

import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;

import java.util.concurrent.atomic.AtomicReference;

public class DefaultStatusUpdater implements StatusUpdater {

  private final ContainerUtil containerUtil;
  private final AtomicReference<ThrottleState> throttle;
  private final AtomicReference<Goal> goal;
  private final TaskStatusManager statusManager;
  private final Supplier<String> containerIdSupplier;

  public DefaultStatusUpdater(final AtomicReference<Goal> goal,
                              final AtomicReference<ThrottleState> throttle,
                              final ContainerUtil containerUtil,
                              final TaskStatusManager statusManager,
                              final Supplier<String> containerIdSupplier) {
    this.goal = goal;
    this.throttle = throttle;
    this.containerUtil = containerUtil;
    this.statusManager = statusManager;
    this.containerIdSupplier = containerIdSupplier;
  }

  /**
   * Persist job status with port mapping.
   */
  @Override
  public void setStatus(final TaskStatus.State status) {
    setStatus(status, containerIdSupplier.get());
  }

  @Override
  public void setStatus(final TaskStatus.State status, final String containerId) {
    statusManager.setStatus(goal.get(), status, throttle.get(),
                            containerId, containerUtil.ports(),
                            containerUtil.getContainerEnvMap());
  }

}
