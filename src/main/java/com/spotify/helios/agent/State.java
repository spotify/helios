/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.Task;

import java.util.Map;

/**
 * Models the desired state of a slave as provided by coordinators and provides a way for a slave
 * to
 * indicate its current state.
 */
public interface State {

  /**
   * Get a map of tasks.
   *
   * @return A map of job id's to tasks.
   */
  Map<JobId, Task> getTasks();

  /**
   * Get a map of the task statuses.
   *
   * @return A map of job ids to task statuses.
   */
  Map<JobId, TaskStatus> getTaskStatuses();

  /**
   * Register task status.
   *
   * @param status The container state.
   */
  void setTaskStatus(final JobId jobId, TaskStatus status);

  /**
   * Get registered task status.
   */
  TaskStatus getTaskStatus(JobId jobId);

  /**
   * Remove a task status.
   *
   * @param jobId The job id.
   */
  void removeTaskStatus(JobId jobId);

  /**
   * Add a listener for changes to the set of tasks.
   *
   * @param listener A listener that will be called when the set of tasks changes.
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
   * A listener for changes to the set of tasks.
   */
  public interface Listener {

    /**
     * The set of tasks changed.
     *
     * @param state This state.
     */
    void tasksChanged(State state);
  }
}
