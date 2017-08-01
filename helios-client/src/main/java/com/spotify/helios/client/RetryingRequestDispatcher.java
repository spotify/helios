/*-
 * -\-\-
 * Helios Client
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

package com.spotify.helios.client;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.spotify.helios.common.Clock;
import com.spotify.helios.common.SystemClock;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RequestDispatcher} that retries.
 */
class RetryingRequestDispatcher implements RequestDispatcher {

  private static final Logger log = LoggerFactory.getLogger(RetryingRequestDispatcher.class);

  private final ListeningScheduledExecutorService executorService;
  private final RequestDispatcher delegate;
  private final Clock clock;
  private final long retryTimeoutMillis;
  private final long delayMillis;

  private RetryingRequestDispatcher(final RequestDispatcher delegate,
                                    final ListeningScheduledExecutorService executorService,
                                    final Clock clock,
                                    final long retryTimeoutMillis,
                                    final long delayMillis) {
    this.delegate = delegate;
    this.executorService = executorService;
    this.clock = clock;
    this.retryTimeoutMillis = retryTimeoutMillis;
    this.delayMillis = delayMillis;
  }

  @Override
  public ListenableFuture<Response> request(final URI uri,
                                            final String method,
                                            final byte[] entityBytes,
                                            final Map<String, List<String>> headers) {
    final long deadline = clock.now().getMillis() + retryTimeoutMillis;
    final SettableFuture<Response> future = SettableFuture.create();
    final Supplier<ListenableFuture<Response>> code = new Supplier<ListenableFuture<Response>>() {
      @Override
      public ListenableFuture<Response> get() {
        return delegate.request(uri, method, entityBytes, headers);
      }
    };
    startRetry(future, code, deadline, delayMillis, uri);
    return future;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  private void startRetry(final SettableFuture<Response> future,
                          final Supplier<ListenableFuture<Response>> code,
                          final long deadline,
                          final long delayMillis,
                          final URI uri) {

    ListenableFuture<Response> codeFuture;
    try {
      codeFuture = code.get();
    } catch (Exception e) {
      log.debug("Failed to connect to {}, retrying in {} seconds.",
          uri.toString(), TimeUnit.MILLISECONDS.toSeconds(delayMillis));
      log.debug("Specific reason for connection failure follows", e);
      handleFailure(future, code, deadline, delayMillis, e, uri);
      return;
    }

    Futures.addCallback(codeFuture, new FutureCallback<Response>() {
      @Override
      public void onSuccess(Response result) {
        future.set(result);
      }

      @Override
      public void onFailure(@NotNull Throwable th) {
        log.warn("Failed to connect to {}, retrying in {} seconds. Exception chain was: {} ",
            uri.toString(), TimeUnit.MILLISECONDS.toSeconds(delayMillis),
            getChainAsString(th));
        log.debug("Specific reason for connection failure follows", th);
        handleFailure(future, code, deadline, delayMillis, th, uri);
      }
    });
  }

  private static String getChainAsString(final Throwable th) {
    final List<Throwable> causalChain = Throwables.getCausalChain(th);
    final List<String> messages = Lists.transform(causalChain, new Function<Throwable, String>() {
      @Override
      public String apply(final Throwable input) {
        return input.toString();
      }
    });
    return Joiner.on(", ").join(messages);
  }

  private void handleFailure(final SettableFuture<Response> future,
                             final Supplier<ListenableFuture<Response>> code,
                             final long deadline,
                             final long delayMillis,
                             final Throwable th,
                             final URI uri) {
    if (clock.now().getMillis() < deadline) {
      if (delayMillis > 0) {
        executorService.schedule(new Runnable() {
          @Override
          public void run() {
            startRetry(future, code, deadline - 1, delayMillis, uri);
          }
        }, delayMillis, TimeUnit.MILLISECONDS);
      } else {
        startRetry(future, code, deadline - 1, delayMillis, uri);
      }
    } else {
      future.setException(th);
    }
  }

  static Builder forDispatcher(RequestDispatcher delegate) {
    return new Builder(delegate);
  }

  public static final class Builder {

    private final RequestDispatcher delegate;
    private ListeningScheduledExecutorService executor;
    private Clock clock = new SystemClock();
    private long retryTimeoutMillis = 60000;
    private long delayMillis = 5000;

    private Builder(final RequestDispatcher delegate) {
      this.delegate = delegate;
    }

    public Builder setExecutor(ScheduledExecutorService executorService) {
      this.executor = MoreExecutors.listeningDecorator(executorService);
      return this;
    }

    /** Defaults to SystemClock. */
    public Builder setClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    /**
     * Set the total amount of time that the RetryingRequestDispatcher allows before giving up on
     * the request. Defaults to 60 seconds.
     */
    public Builder setRetryTimeout(long timeout, TimeUnit unit) {
      this.retryTimeoutMillis = unit.toMillis(timeout);
      return this;
    }

    /**
     * Set how much time the RetryingRequestDispatcher waits to retry the request. Defaults to 5
     * seconds.
     */
    public Builder setDelayOnFailure(long delay, TimeUnit unit) {
      this.delayMillis = unit.toMillis(delay);
      return this;
    }

    public RetryingRequestDispatcher build() {
      return new RetryingRequestDispatcher(
          delegate, executor, clock, retryTimeoutMillis, delayMillis);
    }
  }
}
