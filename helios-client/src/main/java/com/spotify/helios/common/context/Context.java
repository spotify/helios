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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Include call origin information in stacktraces of Callables and Runnables.  For brevity,
 * *able will stand in for both Callable and Runnable below.
 *
 * Normally, if you call .get() on a Future of a *able that fails, you get
 * an ExecutionException wrapping the actual Exception thrown from within the *able.
 * Unfortunately, you do not get the call path that got you to the submission of the *able
 * into the Executor. This class is designed to fix that.  All you do is call
 * makeContextCallable or makeContextRunnable on your *able and submit the result instead of
 * the original *able.
 *
 * So, if you look at ContextTest, if the exception wasn't caught, you'd see (edited for brevity):
 *
 * java.util.concurrent.ExecutionException: java.lang.RuntimeException: No boolean for you
 *     at java.util.concurrent.FutureTask.report(FutureTask.java:122)
 *     at java.util.concurrent.FutureTask.get(FutureTask.java:188)
 *     at com.spotify.helios.common.ContextTest.testContextCallable(ContextTest.java:57)
 * ....
 *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.run(RemoteTestRunner.java:382)
 *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.main(RemoteTestRunner.java:192)
 * Caused by: java.lang.RuntimeException: No boolean for you
 *     at com.spotify.helios.common.ContextTest$1.call(ContextTest.java:55)
 *     at com.spotify.helios.common.ContextTest$1.call(ContextTest.java:1)
 *     at com.spotify.helios.common.Context$1.call(Context.java:44)
 *     at java.util.concurrent.FutureTask.run(FutureTask.java:262)
 *     at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
 *     at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
 *     at java.lang.Thread.run(Thread.java:744)
 * Caused by: com.spotify.helios.common.CallPathToExecutorException
 *     at com.spotify.helios.common.Context.makeContextCallable(Context.java:38)
 *     at com.spotify.helios.common.ContextTest.testContextCallable(ContextTest.java:52)
 *     at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
 * ....
 *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.run(RemoteTestRunner.java:382)
 *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.main(RemoteTestRunner.java:192)
 *
 * Normally, you'd not see anything from the CallPathToExcecutorException line on down.  Here,
 * you can see that it originated in the method testContextCallable, and the normal information
 * lists the call site of the get (third line down from the top).
 */
public final class Context {
  private static final Field causeField;

  static {
    Field fieldValue = null;
    try {
      fieldValue = Throwable.class.getDeclaredField("cause");
      fieldValue.setAccessible(true);
    } catch (NoSuchFieldException | SecurityException e) { // Should never happen
    }
    causeField = fieldValue;
  }

  /**
   * Returns a Callable that will retain the stack trace information about where it
   * originated from.
   *
   * @param in The original Callable.
   */
  public static <T> Callable<T> makeContextCallable(final Callable<T> in) {
    return new ContextCallable<T>(in);
  }

  /**
   * Returns a Runnable that will retain the stack trace information about where it
   * originated from.
   *
   * @param in The original Runnable
   */
  public static Runnable makeContextRunnable(final Runnable in) {
    return new ContextRunnable(in);
  }

  /**
   * Returns an Executor that wraps Runnables before submission to the passed in Executor.
   */
  public static Executor decorate(final Executor executor) {
    return new Executor() {
      @Override
      public void execute(Runnable command) {
        executor.execute(makeContextRunnable(command));
      }
    };
  }

  /**
   * Returns an ExecutorService that wraps *ables before submission to the passed in
   * ExecutorService.
   */
  public static ExecutorService decorate(final ExecutorService executorService) {
    return new ContextExecutorService(executorService);
  }

  /**
   * Returns a ScheduledExecutorService that wraps *ables before submission to the passed in
   * ScheduledExecutorService.
   */
  public static ScheduledExecutorService decorate(final ScheduledExecutorService service) {
    return new ContextScheduledExecutorService(service);
  }

  /**
   * Returns a ListeningExecutorService that wraps *ables before submission to the passed in
   * ListeningExecutorService.
   */
  public static ListeningExecutorService decorate(final ListeningExecutorService service) {
    return new ContextListeningExecutorService(service);
  }

  /**
   * Returns a ListeningScheduledExecutorService that wraps *ables before submission to the passed
   * in ListeningScheduledExecutorService.
   */
  public static ListeningScheduledExecutorService decorate(
      final ListeningScheduledExecutorService service) {
    return new ContextListeningScheduledExecutorService(service);
  }

  /**
   * Set the cause of the root-cause Exception to a new CallPathToExcecutorException
   * that has the trace info in it.
   */
  static void handleException(final StackTraceElement[] trace, final Throwable th) {
    if (causeField != null) { // should *always* be non-null
      try {
        causeField.set(findRootCause(th), new CallPathToExecutorException(trace));
      } catch (IllegalArgumentException | IllegalAccessException e) {
      }
    }
  }

  static StackTraceElement[] getStackContext() {
    // Per http://bugs.java.com/bugdatabase/view_bug.do?bug_id=6375302, this is 10x faster
    // than Thread.getCurrentThread().getStackTrace()
    return new Throwable().getStackTrace();
  }

  private static Throwable findRootCause(Throwable th) {
    // find end of the causality chain
    while (th.getCause() != null) {
      th = th.getCause();
    }
    return th;
  }

  /** Utility function used by the Context*Executor classes */
  static <T> List<Callable<T>> makeContextWrappedCollection(
      Collection<? extends Callable<T>> tasks) {
    final List<Callable<T>> contexted = Lists.newArrayList();
    for (Callable<T> task : tasks) {
      contexted.add(makeContextCallable(task));
    }
    return contexted;
  }
}
