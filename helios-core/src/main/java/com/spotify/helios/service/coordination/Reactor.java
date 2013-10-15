/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.service.coordination;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * A reactor loop that collapses event updates and calls a provided runnable.
 */
public class Reactor {

  private static final Logger log = LoggerFactory.getLogger(Reactor.class);

  private final ExecutorService executor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setDaemon(true).build());

  private final Semaphore semaphore = new Semaphore(0);

  private final Runnable runnable;
  private volatile boolean closed;

  public Reactor(final Runnable runnable) {
    this.runnable = runnable;
    executor.execute(new Loop());
  }

  public void update() {
    semaphore.release();
  }

  public void close() {
    closed = true;
    executor.shutdownNow();
  }

  private class Loop implements Runnable {

    public void run() {
      while (!closed) {
        try {
          semaphore.acquire();
        } catch (InterruptedException e) {
          continue;
        }

        semaphore.drainPermits();

        try {
          runnable.run();
        } catch (Exception e) {
          log.error("reactor runner threw exception", e);
        }
      }
    }
  }
}
