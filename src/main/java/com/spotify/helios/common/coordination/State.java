/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.coordination;

import com.spotify.helios.common.descriptors.AgentJobDescriptor;
import com.spotify.helios.common.descriptors.JobStatus;

import java.util.Map;

/**
 * Models the desired state of a slave as provided by coordinators and provides a way for a slave
 * to indicate its current state.
 */
public interface State {

  /**
   * Get a map of the desired containers.
   *
   * @return A map of container names to descriptors.
   */
  Map<String, AgentJobDescriptor> getJobs();

  /**
   * Register container state.
   *
   * @param name
   * @param state The container state.
   */
  void setJobStatus(final String name, JobStatus state);

  /**
   * Get registered container state.
   * @param name
   * @return
   */
  JobStatus getJobStatus(String name);

  /**
   * Deregister a running container.
   *
   * @param name The container name.
   */
  void removeJobStatus(String name);

  /**
   * Add a listener for changes to the desired set of applications.
   *
   * @param listener A listener that will be called when the desired set of applications changes.
   * @see Listener
   */
  void addListener(Listener listener);

  /**
   * Remove a listener.
   *
   * @see #addListener(Listener)
   */
  void removeListener(Listener listener);

  /**
   * A listener for changes to the desired set of applications.
   */
  public interface Listener {

    /**
     * An application was added to the desired set of applications.
     *
     * @param state This state.
     */
    void containersUpdated(State state);
  }
}
