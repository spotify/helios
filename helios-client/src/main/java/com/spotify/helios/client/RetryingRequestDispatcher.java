/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.client;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;

import com.spotify.helios.common.Clock;
import com.spotify.helios.common.SystemClock;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A {@link RequestDispatcher} that retries. Uses {@link DefaultRequestDispatcher}
 * as a delegate.
 */
class RetryingRequestDispatcher implements RequestDispatcher {

  private static final Logger log = LoggerFactory.getLogger(RetryingRequestDispatcher.class);

  private static final long RETRY_TIMEOUT_MILLIS = SECONDS.toMillis(60);
  private static final long DEFAULT_DELAY = 5;
  private static final TimeUnit DEFAULT_DELAY_TIMEUNIT = SECONDS;

  private final ListeningScheduledExecutorService executorService;
  private final RequestDispatcher delegate;
  private final Clock clock;
  private final long delay;
  private final TimeUnit delayTimeUnit;

  RetryingRequestDispatcher(final Supplier<List<Endpoint>> endpointSupplier,
                            final String user,
                            final ListeningScheduledExecutorService executorService) {
    this(new DefaultRequestDispatcher(endpointSupplier, user, executorService), executorService,
         new SystemClock(), DEFAULT_DELAY, DEFAULT_DELAY_TIMEUNIT);
  }

  RetryingRequestDispatcher(final RequestDispatcher delegate,
                            final ListeningScheduledExecutorService executorService,
                            final Clock clock,
                            final long delay,
                            final TimeUnit delayTimeUnit) {
    this.delegate = delegate;
    this.executorService = executorService;
    this.clock = clock;
    this.delay = delay;
    this.delayTimeUnit = delayTimeUnit;
  }

  @Override
  public ListenableFuture<Response> request(final URI uri,
                                            final String method,
                                            final byte[] entityBytes,
                                            final Map<String, List<String>> headers) {
    final long deadline = clock.now().getMillis() + RETRY_TIMEOUT_MILLIS;
    final SettableFuture<Response> future = SettableFuture.create();
    final Supplier<ListenableFuture<Response>> code = new Supplier<ListenableFuture<Response>>() {
      @Override
      public ListenableFuture<Response> get() {
        return delegate.request(uri, method, entityBytes, headers);
      }
    };
    startRetry(future, code, deadline, delay, delayTimeUnit, Predicates.<Response>alwaysTrue());
    return future;
  }

  @Override
  public void close() {
    delegate.close();
  }

  private void startRetry(final SettableFuture<Response> future,
                          final Supplier<ListenableFuture<Response>> code,
                          final long deadline,
                          final long delay,
                          final TimeUnit timeUnit,
                          final Predicate<Response> retryCondition) {

    ListenableFuture<Response> codeFuture;
    try {
      codeFuture = code.get();
    } catch (Exception e) {
      handleFailure(future, code, deadline, delay, timeUnit, retryCondition, e);
      return;
    }

    Futures.addCallback(codeFuture, new FutureCallback<Response>() {
      @Override
      public void onSuccess(Response result) {
        if (retryCondition.apply(result)) {
          future.set(result);
        } else {
          RuntimeException exception = new RuntimeException("Failed retry condition");
          handleFailure(future, code, deadline, delay, timeUnit, retryCondition, exception);
        }
      }

      @Override
      public void onFailure(@NotNull Throwable t) {
        log.warn("Failed to connect, retrying in {} seconds.",
                 timeUnit.convert(delay, TimeUnit.SECONDS));
        handleFailure(future, code, deadline, delay, timeUnit, retryCondition, t);
      }
    });
  }

  private void handleFailure(final SettableFuture<Response> future,
                                 final Supplier<ListenableFuture<Response>> code,
                                 final long deadline,
                                 final long delay,
                                 final TimeUnit timeUnit,
                                 final Predicate<Response> retryCondition,
                                 final Throwable t) {
    if (clock.now().getMillis() < deadline) {
      if (delay > 0) {
        executorService.schedule(new Runnable() {
          @Override
          public void run() {
            startRetry(future, code, deadline - 1, delay, timeUnit, retryCondition);
          }
        }, delay, timeUnit);
      } else {
        startRetry(future, code, deadline - 1, delay, timeUnit, retryCondition);
      }
    } else {
      future.setException(t);
    }
  }
}
