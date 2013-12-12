/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

import com.google.common.collect.Sets;

import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;

import java.util.Set;

import static java.lang.String.format;

public class JobValidator {

  public Set<String> validate(final Job job) {
    final Set<String> errors = Sets.newHashSet();

    // Check that the job id is correct
    final JobId recomputedId = job.toBuilder().build().getId();
    if (!recomputedId.equals(job.getId())) {
      errors.add(format("Id mismatch: %s != %s", job.getId(), recomputedId));
    }

    // Check that there's not external port collission
    final Set<Integer> externalPorts = Sets.newHashSet();
    for (final PortMapping mapping : job.getPorts().values()) {
      if (externalPorts.contains(mapping.getExternalPort())) {
        errors.add(format("Duplicate external port mapping: %s", mapping.getExternalPort()));
      }
      externalPorts.add(mapping.getExternalPort());
    }

    return errors;
  }
}
