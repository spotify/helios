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

package com.spotify.helios.auth;

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.client.RequestDispatcher;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuthProviderSelectorTest {

  private final RequestDispatcher dispatcher = mock(RequestDispatcher.class);
  private final AuthProvider provider1 = mock(AuthProvider.class);
  private final AuthProvider provider2 = mock(AuthProvider.class);
  private final AuthProvider.Factory providerFactory1 = mock(AuthProvider.Factory.class);
  private final AuthProvider.Factory providerFactory2 = mock(AuthProvider.Factory.class);
  private AuthProviderSelector selector;

  @Before
  public void setUp() {
    when(providerFactory1.create(any(RequestDispatcher.class))).thenReturn(provider1);
    when(providerFactory2.create(any(RequestDispatcher.class))).thenReturn(provider2);
    when(provider1.renewAuthorizationHeader(anyString())).thenReturn(immediateFuture("scheme1 creds"));
    when(provider2.renewAuthorizationHeader(anyString())).thenReturn(immediateFuture("scheme2 creds"));
    when(provider1.currentAuthorizationHeader()).thenReturn("scheme1 creds");
    when(provider2.currentAuthorizationHeader()).thenReturn("scheme2 creds");

    this.selector = new AuthProviderSelector(
        dispatcher, ImmutableMap.of("scheme1", providerFactory1,
                                    "scheme2", providerFactory2));
  }

  @Test
  public void testCurrentAuthNullBeforeRenewal() {
    assertNull(selector.currentAuthorizationHeader());
  }

  @Test
  public void testRenewAuth() throws Exception {
    assertEquals("scheme1 creds", selector.renewAuthorizationHeader("scheme1").get());
    verify(provider1).renewAuthorizationHeader("scheme1");

    when(provider1.renewAuthorizationHeader(anyString())).thenReturn(immediateFuture("scheme1 creds2"));
    when(provider1.currentAuthorizationHeader()).thenReturn("scheme1 creds2");

    assertEquals("scheme1 creds2", selector.renewAuthorizationHeader("scheme1").get());
    verify(provider1, times(2)).renewAuthorizationHeader("scheme1");
    assertEquals(selector.currentAuthorizationHeader(), "scheme1 creds2");
  }

  @Test
  public void testAuthSchemeSwitching() throws Exception {
    assertEquals("scheme1 creds", selector.renewAuthorizationHeader("scheme1").get());
    verify(provider1).renewAuthorizationHeader("scheme1");
    assertEquals(selector.currentAuthorizationHeader(), "scheme1 creds");

    assertEquals("scheme2 creds", selector.renewAuthorizationHeader("scheme2").get());
    verify(provider2).renewAuthorizationHeader("scheme2");
    assertEquals(selector.currentAuthorizationHeader(), "scheme2 creds");
  }

  @Test
  public void testAuthHeaderWithParams() throws Exception {
    assertEquals("scheme1 creds", selector.renewAuthorizationHeader("scheme1 realm='foo.bar'").get());
    verify(provider1).renewAuthorizationHeader(anyString());
  }

  @Test
  public void testUnsupportedAuthScheme() throws Exception {
    Throwable cause = null;
    try {
      selector.renewAuthorizationHeader("unsupported scheme").get();
    } catch (ExecutionException e) {
      cause = e.getCause();
    }

    assertNotNull(cause);
    assertTrue(cause instanceof IllegalArgumentException);
    assertNull(selector.currentAuthorizationHeader());
  }
}
