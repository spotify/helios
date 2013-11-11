/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * A reactor loop that collapses event updates and calls a provided callback.
 */
public class Reactor {

  private static final Logger log = LoggerFactory.getLogger(Reactor.class);

  public static final ThreadFactory THREAD_FACTORY =
      new ThreadFactoryBuilder().setNameFormat("helios-reactor-%d").setDaemon(true).build();

  private final ExecutorService executor = Executors.newSingleThreadExecutor(THREAD_FACTORY);

  private final Semaphore semaphore = new Semaphore(0);

  private final Runnable callback;
  private volatile boolean closed;
  private final long timeoutMillis;

  /**
   * Create a reactor that calls the provided callback with the specified timeout interval.
   *
   * @param callback      The callback to call.
   * @param timeoutMillis The timeout in millis after which the callback should be called even if
   *                      there has been no updates.
   */
  public Reactor(final Runnable callback, final long timeoutMillis) {
    this.callback = callback;
    this.timeoutMillis = timeoutMillis;
    executor.execute(new Loop());
  }

  /**
   * Signal an update. The callback will be called at least once after this method is called.
   */
  public void update() {
    semaphore.release();
  }

  /**
   * Stop this reactor.
   */
  public void close() {
    closed = true;
    executor.shutdownNow();
  }

  private class Loop implements Runnable {

    public void run() {
      while (!closed) {
        try {
          semaphore.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          continue;
        }

        semaphore.drainPermits();

        try {
          callback.run();
        } catch (Exception e) {
          log.error("reactor runner threw exception", e);
        }
      }
    }
  }
}
