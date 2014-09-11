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

package com.spotify.helios.common.context;

import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.spotify.helios.common.context.Context.makeContextCallable;
import static com.spotify.helios.common.context.Context.makeContextRunnable;

class ContextListeningScheduledExecutorService extends ContextListeningExecutorService
    implements ListeningScheduledExecutorService {

  private final ListeningScheduledExecutorService service;

  ContextListeningScheduledExecutorService(ListeningScheduledExecutorService service) {
    super(service);
    this.service = service;
  }

  @Override
  public ListenableScheduledFuture<?> schedule(Runnable command, long timeout, TimeUnit unit) {
    return service.schedule(makeContextRunnable(command), timeout, unit);
  }

  @Override
  public <V> ListenableScheduledFuture<V> schedule(Callable<V> task, long timeout, TimeUnit unit) {
    return service.schedule(makeContextCallable(task), timeout, unit);
  }

  @Override
  public ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay,
      long period, TimeUnit unit) {
    return service.scheduleAtFixedRate(makeContextRunnable(command), initialDelay, period, unit);
  }

  @Override
  public ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay,
      long delay, TimeUnit unit) {
    return service.scheduleWithFixedDelay(makeContextRunnable(command), initialDelay, delay, unit);
  }
}
