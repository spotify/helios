/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.spotify.helios.common.coordination.State;
import com.spotify.helios.common.descriptors.AgentJobDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

abstract class AbstractState implements State {

  private static final Logger log = LoggerFactory.getLogger(AbstractState.class);

  private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();
  private final ConcurrentMap<String, AgentJobDescriptor> jobs = Maps.newConcurrentMap();

  @Override
  public void addListener(final Listener listener) {
    listeners.add(listener);
    listener.containersUpdated(this);
  }

  @Override
  public void removeListener(final Listener listener) {
    listeners.remove(listener);
  }

  @Override
  public Map<String, AgentJobDescriptor> getJobs() {
    return ImmutableMap.copyOf(jobs);
  }

  protected void doAddJob(final String name, final AgentJobDescriptor descriptor) {
    log.debug("adding container: name={}, descriptor={}", name, descriptor);
    jobs.put(name, descriptor);
    fireContainersUpdated();
  }

  protected void doUpdateJob(final String name, final AgentJobDescriptor descriptor) {
    log.debug("updating container: name={}, descriptor={}", name, descriptor);
    jobs.put(name, descriptor);
    fireContainersUpdated();
  }

  protected AgentJobDescriptor doRemoveJob(final String name) {
    log.debug("removing application: name={}", name);
    final AgentJobDescriptor descriptor;
    descriptor = jobs.remove(name);
    fireContainersUpdated();
    return descriptor;
  }

  private void fireContainersUpdated() {
    for (final Listener listener : listeners) {
      try {
        listener.containersUpdated(this);
      } catch (Exception e) {
        log.error("listener threw exception", e);
      }
    }
  }
}
