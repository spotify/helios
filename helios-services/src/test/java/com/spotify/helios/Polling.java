package com.spotify.helios;

import org.junit.Ignore;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.System.nanoTime;

@Ignore
public class Polling {

  public static <T> T await(final long timeout, final TimeUnit timeUnit, final Callable<T> callable)
      throws Exception {
    final long deadline = nanoTime() + timeUnit.toNanos(timeout);
    while (nanoTime() < deadline) {
      final T value = callable.call();
      if (value != null) {
        return value;
      }
      Thread.sleep(500);
    }
    throw new TimeoutException();
  }
}
