/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
public class ServicePortParameters extends Descriptor {

  @Override
  public boolean equals(final Object obj) {
    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return "{}";
  }
}
