package com.spotify.helios.common.descriptors;

// If you edit this, you'll want to do something reasonable in RestartPolicy
public enum ThrottleState {
  NO,
  FLAPPING,
  IMAGE_MISSING,
  IMAGE_PULL_FAILED
}