/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.agent;

import com.google.common.util.concurrent.MoreExecutors;

import com.spotify.docker.client.ImageNotFoundException;
import com.spotify.docker.client.ImagePullFailedException;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.spotify.helios.common.descriptors.TaskStatus.State.CREATING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.PULLING_IMAGE;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STARTING;
import static com.spotify.helios.common.descriptors.ThrottleState.FLAPPING;
import static com.spotify.helios.common.descriptors.ThrottleState.IMAGE_MISSING;
import static com.spotify.helios.common.descriptors.ThrottleState.IMAGE_PULL_FAILED;
import static com.spotify.helios.common.descriptors.ThrottleState.NO;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A monitor for {@link TaskRunner}, processing events into observations about the health of a task,
 * e.g. whether it's flapping etc. This information is published using the {@link StatusUpdater} and
 * a {@link ThrottleState} is made available for supervisors to act on.
 */
public class TaskMonitor implements TaskRunner.Listener {

  private static final Logger log = LoggerFactory.getLogger(TaskMonitor.class);

  private static final ScheduledExecutorService scheduler =
      MoreExecutors.getExitingScheduledExecutorService(
          (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1), 0, SECONDS);

  private final FlapController flapController;
  private final StatusUpdater statusUpdater;

  private volatile ScheduledFuture<?> flapTimeout;

  private ThrottleState imageFailure;

  public TaskMonitor(final FlapController flapController,
                     final StatusUpdater statusUpdater) {
    this.flapController = flapController;
    this.statusUpdater = statusUpdater;
  }

  @Override
  public void failed(final Throwable t) {
    if (t instanceof InterruptedException) {
      return;
    }
    if (t instanceof ImageNotFoundException) {
      imageFailure(IMAGE_MISSING);
    } else if (t instanceof ImagePullFailedException) {
      imageFailure(IMAGE_PULL_FAILED);
    }
    updateState(FAILED);
  }

  @Override
  public void pulling() {
    updateState(PULLING_IMAGE);
  }

  @Override
  public void creating() {
    resetImageFailure();
    updateState(CREATING);
  }

  @Override
  public void created(final String containerId) {
    resetImageFailure();
    statusUpdater.setContainerId(containerId);
  }

  @Override
  public void starting() {
    resetImageFailure();
    updateState(STARTING);
  }

  @Override
  public void started() {
    resetImageFailure();
  }

  @Override
  public void running() {
    flapController.started();
    if (flapTimeout != null) {
      flapTimeout.cancel(false);
    }
    if (flapController.isFlapping()) {
      flapTimeout = scheduler.schedule(new UpdateThrottle(),
                                       flapController.millisLeftToUnflap(), MILLISECONDS);
    }
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
    if (imageFailure != null) {
      imageFailure = null;
      updateThrottle();
    }
  }

  private void updateThrottle() {
    statusUpdater.setThrottleState(throttle());
  }

  private void updateState(final TaskStatus.State state) {
    statusUpdater.setState(state);
    try {
      statusUpdater.update();
    } catch (InterruptedException e) {
      // TODO: propagate instead?
      Thread.currentThread().interrupt();
    }
  }

  public ThrottleState throttle() {
    if (imageFailure != null) {
      // Image failures take precedence
      return imageFailure;
    } else {
      return flapController.isFlapping() ? FLAPPING : NO;
    }
  }

  private class UpdateThrottle implements Runnable {

    @Override
    public void run() {
      updateThrottle();
      try {
        statusUpdater.update();
      } catch (InterruptedException ignore) {
      }
    }
  }
}
