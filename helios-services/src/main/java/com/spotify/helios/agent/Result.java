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

package com.spotify.helios.agent;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

/**
 * Helper for getting the result of a {@link ListenableFuture}.
 */
public class Result<V> implements FutureCallback<V> {

  private volatile boolean done;
  private volatile V result;
  private volatile Throwable exception;

  public Result(final ListenableFuture<V> future) {
    Futures.addCallback(future, this);
  }

  @Override
  public void onSuccess(@Nullable final V r) {
    done = true;
    result = r;
  }

  @Override
  public void onFailure(final Throwable t) {
    done = true;
    exception = t;
  }

  public boolean isDone() {
    return done;
  }

  public boolean isSuccess() {
    return isDone() && result != null;
  }

  public boolean isFailure() {
    return isDone() && exception != null;
  }

  public V getResult() {
    return result;
  }

  public Throwable getException() {
    return exception;
  }

  public static <V> Result<V> of(final ListenableFuture<V> future) {
    return new Result<>(future);
  }
}
