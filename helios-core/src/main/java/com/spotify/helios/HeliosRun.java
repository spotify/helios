/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios;

/**
 * This is an abstraction for a way of running and
 * stopping the Helios Service.
 *
 * Implementors must ensure that it is safe to call
 * the {@link #stop} method even if the start
 * method throws an exception
 */
public interface HeliosRun {

  void start() throws Exception;

  void stop();
}
