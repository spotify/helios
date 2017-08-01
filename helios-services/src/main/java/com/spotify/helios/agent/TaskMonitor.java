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

import static com.spotify.helios.common.descriptors.TaskStatus.State.CREATING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.HEALTHCHECKING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.PULLING_IMAGE;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STARTING;
import static com.spotify.helios.common.descriptors.ThrottleState.FLAPPING;
import static com.spotify.helios.common.descriptors.ThrottleState.IMAGE_MISSING;
import static com.spotify.helios.common.descriptors.ThrottleState.IMAGE_PULL_FAILED;
import static com.spotify.helios.common.descriptors.ThrottleState.NO;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.docker.client.exceptions.ImageNotFoundException;
import com.spotify.docker.client.exceptions.ImagePullFailedException;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A monitor for {@link TaskRunner}, processing events into observations about the health of a task,
 * e.g. whether it's flapping etc. This information is published using the {@link StatusUpdater} and
 * a {@link ThrottleState} is made available for supervisors to act on.
 */
public class TaskMonitor implements TaskRunner.Listener, Closeable {

  private static final Logger log = LoggerFactory.getLogger(TaskMonitor.class);

  private final JobId jobId;
  private final ScheduledExecutorService scheduler;
  private final FlapController flapController;
  private final StatusUpdater statusUpdater;

  private volatile ScheduledFuture<?> flapTimeout;

  private ThrottleState imageFailure;
  private ThrottleState throttle = NO;

  public TaskMonitor(final JobId jobId, final FlapController flapController,
                     final StatusUpdater statusUpdater) {
    this.jobId = jobId;
    this.flapController = flapController;
    this.statusUpdater = statusUpdater;

    final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    // Let core threads time out to avoid unnecessarily keeping a flapping state check thread alive
    // for the majority of tasks that do not flap.
    executor.setKeepAliveTime(5, SECONDS);
    executor.allowCoreThreadTimeOut(true);
    this.scheduler = MoreExecutors.getExitingScheduledExecutorService(executor, 0, SECONDS);
  }

  /**
   * Get the current task throttle as derived from task runner events.
   *
   * @return The throttle state.
   */
  public ThrottleState throttle() {
    return throttle;
  }

  @Override
  public void close() {
    scheduler.shutdownNow();
  }

  @Override
  public void failed(final Throwable th, String containerError) {
    if (th instanceof InterruptedException) {
      // Ignore failures due to interruptions as they're used when tearing down the agent and do
      // not indicate actual runner failures.
      return;
    }
    if (th instanceof ImageNotFoundException) {
      imageFailure(IMAGE_MISSING);
    } else if (th instanceof ImagePullFailedException) {
      imageFailure(IMAGE_PULL_FAILED);
    }
    // Don't use updateState() to avoid calling statusUpdater.update() twice in a row.
    statusUpdater.setState(FAILED);
    statusUpdater.setContainerError(containerError);
    // Commit and push a new status
    try {
      statusUpdater.update();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void pulling() {
    updateState(PULLING_IMAGE);
  }

  @Override
  public void pulled() {
  }

  @Override
  public void pullFailed() {
  }

  @Override
  public void creating() {
    updateState(CREATING);
  }

  @Override
  public void created(final String containerId) {
    // If we managed to create a container, any previous image failure has been resolved
    resetImageFailure();
    statusUpdater.setContainerId(containerId);
  }

  @Override
  public void starting() {
    // If we managed to create a container, any previous image failure has been resolved
    resetImageFailure();
    updateState(STARTING);
  }

  @Override
  public void started() {
    // If we managed to start a container, any previous image failure has been resolved
    resetImageFailure();
  }

  @Override
  public void healthChecking() {
    // If the container is running a health check, any previous image failure has been resolved
    resetImageFailure();
    updateState(HEALTHCHECKING);
  }

  @Override
  public void running() {
    flapController.started();
    // If the container is running, any previous image failure has been resolved
    resetImageFailure();
    updateState(RUNNING);
  }

  @Override
  public void exited(final int code) {
    flapController.exited();
    updateThrottle();
    updateState(EXITED);
  }

  private void imageFailure(final ThrottleState imageFailure) {
    this.imageFailure = imageFailure;
    updateThrottle();
  }

  private void resetImageFailure() {
    imageFailure = null;
    updateThrottle();
  }

  /**
   * Derive a new throttle state and propagate it if needed. If flapping, schedule a future check
   * to see if we're still flapping and potentially reset the flapping state.
   */
  private boolean updateThrottle() {

    // Derive new throttle state
    final ThrottleState newThrottle;
    final boolean flapping = flapController.isFlapping();
    if (imageFailure != null) {
      // Image failures take precedence
      newThrottle = imageFailure;
    } else {
      newThrottle = flapping ? FLAPPING : NO;
    }

    // If the throttle state changed, propagate it
    final boolean updated;
    if (!Objects.equals(throttle, newThrottle)) {
      log.info("throttle state change: {}: {} -> {}", jobId, throttle, newThrottle);
      throttle = newThrottle;
      statusUpdater.setThrottleState(throttle);
      updated = true;
    } else {
      updated = false;
    }

    // If we're flapping, schedule a future check to potentially reset the flapping state
    if (flapping) {
      if (flapTimeout != null) {
        flapTimeout.cancel(false);
      }
      flapTimeout = scheduler.schedule(new UpdateThrottle(),
          flapController.millisLeftToUnflap(), MILLISECONDS);
    }

    // Let the caller know if they need to commit the state change
    return updated;
  }

  /**
   * Propagate a new task state by setting it and committing the status update.
   */
  private void updateState(final TaskStatus.State state) {
    statusUpdater.setState(state);
    // Commit and push a new status
    try {
      statusUpdater.update();
    } catch (InterruptedException e) {
      // TODO: propagate interrupt instead?
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Used to schedule flapping state updates while task is running.
   *
   * @see #updateThrottle()
   */
  private class UpdateThrottle implements Runnable {

    @Override
    public void run() {
      if (updateThrottle()) {
        try {
          statusUpdater.update();
        } catch (InterruptedException ignore) {
          // ignore
        }
      }
    }
  }
}
