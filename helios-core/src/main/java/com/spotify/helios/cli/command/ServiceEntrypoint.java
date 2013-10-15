/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.Entrypoint;
import com.spotify.helios.HeliosRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;

/**
 * And {@link Entrypoint} for running the Helios Service via an {@link com.spotify.helios.HeliosRun}
 * instance.
 *
 * This {@link Entrypoint} will start the {@link com.spotify.helios.HeliosRun} instance and wait
 * until the run latch releases before it will stop the instance and return.
 *
 * {@code HeliosRun.stop()} will always be called. Even if {@code HeliosRun.start()} throws an
 * exception.
 */
class ServiceEntrypoint implements Entrypoint {

  private static final Logger log = LoggerFactory.getLogger(ServiceEntrypoint.class);

  private final HeliosRun heliosRun;

  ServiceEntrypoint(HeliosRun heliosRun) {
    this.heliosRun = checkNotNull(heliosRun);
  }

  /**
   * Creates a new {@link com.spotify.helios.HeliosRun} instance and stars it. Then blocks until the
   * runLatch is released before it stops the instance.
   *
   * @param runLatch A latch that will be awaited before the {@link com.spotify.helios.HeliosRun}
   *                 instance is stopped.
   * @return 0 if the {@link com.spotify.helios.HeliosRun} instance started successfully.
   */
  @Override
  public int enter(CountDownLatch runLatch) {
    try {
      heliosRun.start();
      awaitUninterruptibly(runLatch);
      return 0;
    } catch (Exception e) {
      log.error("Failed to run", e);
      return 1;
    } finally {
      heliosRun.stop();
    }
  }
}
