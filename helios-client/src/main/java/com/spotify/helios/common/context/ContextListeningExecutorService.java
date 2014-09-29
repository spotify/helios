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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.spotify.helios.common.context.Context.makeContextCallable;
import static com.spotify.helios.common.context.Context.makeContextRunnable;
import static com.spotify.helios.common.context.Context.makeContextWrappedCollection;

class ContextListeningExecutorService implements ListeningExecutorService {
  private final ListeningExecutorService service;

  ContextListeningExecutorService(ListeningExecutorService service) {
    this.service = service;
  }

  @Override
  public void shutdown() {
    service.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return service.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return service.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return service.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return service.awaitTermination(timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException,
      ExecutionException {
    return service.invokeAny(makeContextWrappedCollection(tasks));
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return service.invokeAny(makeContextWrappedCollection(tasks), timeout, unit);
  }

  @Override
  public void execute(Runnable command) {
    service.execute(makeContextRunnable(command));

  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return service.invokeAll(makeContextWrappedCollection(tasks));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
      TimeUnit unit) throws InterruptedException {
    return service.invokeAll(makeContextWrappedCollection(tasks), timeout, unit);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    return service.submit(makeContextCallable(task));
  }

  @Override
  public ListenableFuture<?> submit(Runnable command) {
    return service.submit(makeContextRunnable(command));
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable command, T result) {
    return service.submit(makeContextRunnable(command), result);
  }
}
