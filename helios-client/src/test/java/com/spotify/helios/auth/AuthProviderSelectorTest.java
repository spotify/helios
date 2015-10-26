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

package com.spotify.helios.auth;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthProviderSelectorTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private final AuthProvider provider1 = mock(AuthProvider.class);
  private final AuthProvider provider2 = mock(AuthProvider.class);
  private final AuthProvider.Factory providerFactory1 = mock(AuthProvider.Factory.class);
  private final AuthProvider.Factory providerFactory2 = mock(AuthProvider.Factory.class);
  private final ImmutableMap<String, AuthProvider.Factory> factories = ImmutableMap.of(
      "scheme1", providerFactory1,
      "scheme2", providerFactory2);
  private final AuthProvider.Context context = mock(AuthProvider.Context.class);

  @Before
  public void setUp() {
    when(providerFactory1.create(anyString(), any(AuthProvider.Context.class)))
        .thenReturn(provider1);
    when(providerFactory2.create(anyString(), any(AuthProvider.Context.class)))
        .thenReturn(provider2);
  }

  @Test
  public void testWithoutParams() {
    final AuthProviderSelector selector = new AuthProviderSelector(factories);
    assertSame(provider1, selector.create("scheme1", context));
  }

  @Test
  public void testWithParams() {
    final AuthProviderSelector selector = new AuthProviderSelector(factories);
    assertSame(provider2, selector.create("scheme2", context));
  }

  @Test
  public void testThrowsOnUnsupportedAuthScheme() {
    final AuthProviderSelector selector = new AuthProviderSelector(factories);
    exception.expect(IllegalArgumentException.class);
    selector.create("scheme3", context);
  }

  @Test
  public void testFactoryReturnsNull() {
    when(providerFactory1.create(anyString(), any(AuthProvider.Context.class))).thenReturn(null);
    final AuthProviderSelector selector = new AuthProviderSelector(factories);
    assertNull(selector.create("scheme1", context));
  }
}
