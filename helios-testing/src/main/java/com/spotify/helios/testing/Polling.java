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

package com.spotify.helios.testing;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static java.lang.System.nanoTime;

class Polling {

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
