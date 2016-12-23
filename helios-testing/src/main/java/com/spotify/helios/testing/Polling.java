/*-
 * -\-\-
 * Helios Testing Library
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

package com.spotify.helios.testing;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static java.lang.System.nanoTime;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class Polling {

  static <T> T await(final long timeout, final TimeUnit timeUnit,
                     final String message, final Callable<T> callable) throws Exception {
    final long deadline = nanoTime() + timeUnit.toNanos(timeout);
    while (nanoTime() < deadline) {
      final T value = callable.call();
      if (value != null) {
        return value;
      }
      Thread.sleep(500);
    }
    throw new TimeoutException(String.format(message, timeout, timeUnit.toString().toLowerCase()));
  }

  public static <T> T awaitUnchecked(final long timeout, final TimeUnit timeUnit,
                                     final String message, final Callable<T> callable)
      throws TimeoutException {
    try {
      return await(timeout, timeUnit, message, callable);
    } catch (Throwable e) {
      propagateIfInstanceOf(e, TimeoutException.class);
      throw propagate(e);
    }
  }
}
