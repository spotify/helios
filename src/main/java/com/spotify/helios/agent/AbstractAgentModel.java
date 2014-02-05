/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CopyOnWriteArrayList;

abstract class AbstractAgentModel implements AgentModel {

  private static final Logger log = LoggerFactory.getLogger(AbstractAgentModel.class);

  private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

  @Override
  public void addListener(final Listener listener) {
    listeners.add(listener);
    listener.tasksChanged(this);
  }

  @Override
  public void removeListener(final Listener listener) {
    listeners.remove(listener);
  }

  protected void fireTasksUpdated() {
    for (final Listener listener : listeners) {
      try {
        listener.tasksChanged(this);
      } catch (Exception e) {
        log.error("listener threw exception", e);
      }
    }
  }
}
