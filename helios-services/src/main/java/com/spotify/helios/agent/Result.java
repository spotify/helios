package com.spotify.helios.agent;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

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
