/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios;

import java.util.concurrent.CountDownLatch;

/**
 * Represents a main entrypoint.
 *
 * An entrypoint could be running the helios service
 * in various modes or running a service control
 * command.
 */
public interface Entrypoint {

  /**
   * The method that enters the entrypoint.
   *
   * @param runLatch A latch that the entrypoint might
   *                 wait for before exiting.
   * @return A status code signalling how the entrypoint was exited.
   * @throws Exception The entrypoint might throw exceptions
   *                   for unexpected failures.
   */
  int enter(CountDownLatch runLatch) throws Exception;
}
