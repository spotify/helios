package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.descriptors.ThrottleState;

import java.util.Map;

/**
 * A status manager that behaves like a real one for testing purposes, but does not talk to ZK.
 */
class FakeTaskStatusManager implements TaskStatusManager {
  private volatile State state;
  private volatile boolean isFlapping;

  @Override
  public void setStatus(Goal goal, State status, ThrottleState throttle, String containerId,
                        Map<String, PortMapping> ports, Map<String, String> env) {
    this.state = status;
  }

  public void setState(State state) {
    this.state = state;
  }

  @Override
  public void updateFlappingState(boolean isFlapping) {
    this.isFlapping = isFlapping;
  }

  @Override
  public State getStatus() {
    return state;
  }

  @Override
  public boolean isFlapping() {
    return isFlapping;
  }
}