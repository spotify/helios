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

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.helios.common.Clock;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.hamcrest.CoreMatchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;

public class RetryingRequestDispatcherTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private final RequestDispatcher delegate = mock(RequestDispatcher.class);
  private final Clock clock = mock(Clock.class);

  private RetryingRequestDispatcher dispatcher;

  @Before
  public void setUp() {
    dispatcher = RetryingRequestDispatcher.forDispatcher(delegate)
        .setExecutor(newSingleThreadScheduledExecutor())
        .setClock(clock)
        .setDelayOnFailure(0, SECONDS)
        .build();
  }

  @Test
  public void testSuccess() throws Exception {
    when(delegate.request(any(URI.class), anyString(), any(byte[].class),
        Matchers.<Map<String, List<String>>>any()))
        .thenReturn(Futures.<Response>immediateFuture(null));

    when(clock.now()).thenReturn(new Instant(0));

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

    when(clock.now()).thenReturn(new Instant(0));

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

    when(clock.now()).thenReturn(new Instant(0)).thenReturn(new Instant(80000));

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
