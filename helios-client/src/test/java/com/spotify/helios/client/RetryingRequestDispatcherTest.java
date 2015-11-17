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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import com.spotify.helios.common.Clock;

import org.hamcrest.CoreMatchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RetryingRequestDispatcherTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private static RequestDispatcher delegate;
  private static Clock clock;

  @Before
  public void setup() {
    delegate = mock(RequestDispatcher.class);
    clock = mock(Clock.class);
  }

  @Test
  public void testSuccess() throws Exception {
    when(delegate.request(any(URI.class), anyString(), any(byte[].class),
                          Matchers.<Map<String, List<String>>>any()))
        .thenReturn(Futures.<Response>immediateFuture(null));
    final ListeningScheduledExecutorService executorService = mock(
        ListeningScheduledExecutorService.class);
    when(clock.now()).thenReturn(new Instant(0));

    final RetryingRequestDispatcher dispatcher =
        new RetryingRequestDispatcher(delegate, executorService, clock, 0, SECONDS);
    dispatcher.request(new URI("http://example.com"), "GET", null,
                       Collections.<String, List<String>>emptyMap());

    // Verify the delegate was only called once if it returns successfully on the first try
    verify(delegate, times(1)).request(any(URI.class), anyString(), any(byte[].class),
                                       Matchers.<Map<String, List<String>>>any());
  }

  @Test
  public void testSuccessOnRetry() throws Exception {
    when(delegate.request(any(URI.class), anyString(), any(byte[].class),
                          Matchers.<Map<String, List<String>>>any()))
        .thenReturn(Futures.<Response>immediateFailedFuture(new IOException()))
        .thenReturn(Futures.<Response>immediateFuture(null));

    final ListeningScheduledExecutorService executorService =
        MoreExecutors.listeningDecorator(newSingleThreadScheduledExecutor());
    when(clock.now()).thenReturn(new Instant(0));

    final RetryingRequestDispatcher dispatcher =
        new RetryingRequestDispatcher(delegate, executorService, clock, 0, SECONDS);
    dispatcher.request(new URI("http://example.com"), "GET", null,
                       Collections.<String, List<String>>emptyMap());

    // Verify the delegate was called twice if it returns successfully on the second try before the
    // deadline
    verify(delegate, times(2)).request(any(URI.class), anyString(), any(byte[].class),
                                       Matchers.<Map<String, List<String>>>any());
  }

  @Test
  public void testFailureOnTimeout() throws Exception {
    when(delegate.request(any(URI.class), anyString(), any(byte[].class),
                          Matchers.<Map<String, List<String>>>any()))
        .thenReturn(Futures.<Response>immediateFailedFuture(new IOException()))
        .thenReturn(Futures.<Response>immediateFuture(null));

    final ListeningScheduledExecutorService executorService =
        MoreExecutors.listeningDecorator(newSingleThreadScheduledExecutor());
    when(clock.now()).thenReturn(new Instant(0)).thenReturn(new Instant(80000));

    final RetryingRequestDispatcher dispatcher =
        new RetryingRequestDispatcher(delegate, executorService, clock, 0, SECONDS);
    final ListenableFuture<Response> future = dispatcher.request(
        new URI("http://example.com"), "GET", null, Collections.<String, List<String>>emptyMap());

    // Verify the delegate was only called once if it failed on the first try and the deadline
    // has passed before the second try was attempted.
    verify(delegate, times(1)).request(any(URI.class), anyString(), any(byte[].class),
                                       Matchers.<Map<String, List<String>>>any());
    exception.expect(ExecutionException.class);
    exception.expectCause(CoreMatchers.any(IOException.class));
    future.get();
  }

}
