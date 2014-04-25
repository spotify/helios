package com.spotify.helios;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static java.lang.System.nanoTime;

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

  public static <T> T awaitUnchecked(final long timeout, final TimeUnit timeUnit,
                                     final Callable<T> callable) throws TimeoutException {
    try {
      return await(timeout, timeUnit, callable);
    } catch (Throwable e) {
      propagateIfInstanceOf(e, TimeoutException.class);
      throw propagate(e);
    }
  }
}
