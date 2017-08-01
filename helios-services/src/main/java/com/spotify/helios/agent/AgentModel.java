/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.Task;
import com.spotify.helios.common.descriptors.TaskStatus;
import java.util.Map;

/**
 * Models the desired state of a host as provided by masters and provides a way for an agent to
 * indicate its current state.
 */
public interface AgentModel {

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
   * @param jobId  The job ID.
   * @param status The job state.
   *
   * @throws InterruptedException If the thread is interrupted.
   */
  void setTaskStatus(final JobId jobId, TaskStatus status) throws InterruptedException;

  /**
   * Get registered task status.
   *
   * @param jobId The job ID.
   *
   * @return The job state.
   */
  TaskStatus getTaskStatus(JobId jobId);

  /**
   * Remove a task status.
   *
   * @param jobId The job id.
   *
   * @throws InterruptedException If the thread is interrupted.
   */
  void removeTaskStatus(JobId jobId) throws InterruptedException;

  /**
   * Add a listener for changes to the set of tasks.
   *
   * @param listener A listener that will be called when the set of tasks changes.
   *
   * @see Listener
   */
  void addListener(Listener listener);

  /**
   * Remove a listener.
   *
   * @param listener The listener to remove.
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
     * @param model This state.
     */
    void tasksChanged(AgentModel model);
  }
}
