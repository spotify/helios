/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.AgentJobDescriptor;
import com.spotify.helios.common.descriptors.JobStatus;

import java.util.Map;

/**
 * Models the desired state of a slave as provided by coordinators and provides a way for a slave to
 * indicate its current state.
 */
public interface State {

  /**
   * Get a map of the desired jobs.
   *
   * @return A map of jobs names to descriptors.
   */
  Map<String, AgentJobDescriptor> getJobs();

  /**
   * Get a map of the job statuses.
   *
   * @return A map of job names to statuses.
   */
  Map<String, JobStatus> getJobStatuses();

  /**
   * Register job status.
   *
   * @param name
   * @param state The container state.
   */
  void setJobStatus(final String name, JobStatus state);

  /**
   * Get registered job status.
   * @param name
   * @return
   */
  JobStatus getJobStatus(String name);

  /**
   * Remove a job status.
   *
   * @param name The job name.
   */
  void removeJobStatus(String name);

  /**
   * Add a listener for changes to the desired set of jobs.
   *
   * @param listener A listener that will be called when the desired set of jobs changes.
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
    void jobsUpdated(State state);
  }
}
