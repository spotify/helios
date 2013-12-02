/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

import com.google.common.collect.Sets;

import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.PortMapping;

import java.util.Set;

public class JobValidator {

  public Set<String> validate(final Job job) {
    final Set<String> errors = Sets.newHashSet();

    // Check that there's not external port collission
    final Set<Integer> externalPorts = Sets.newHashSet();
    for (final PortMapping mapping : job.getPorts().values()) {
      if (externalPorts.contains(mapping.getExternalPort())) {
        errors.add(String.format("Duplicate external port mapping: %s", mapping.getExternalPort()));
      }
      externalPorts.add(mapping.getExternalPort());
    }

    return errors;
  }
}
