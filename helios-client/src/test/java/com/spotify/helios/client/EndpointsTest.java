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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import org.apache.http.conn.DnsResolver;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class EndpointsTest {

  private static final InetAddress IP_A = InetAddresses.forString("1.2.3.4");
  private static final InetAddress IP_B = InetAddresses.forString("2.3.4.5");
  private static final InetAddress IP_C = InetAddresses.forString("3.4.5.6");
  private static final InetAddress IP_D = InetAddresses.forString("4.5.6.7");

  private static final InetAddress[] IPS_1 = new InetAddress[]{ IP_A, IP_B };
  private static final InetAddress[] IPS_2 = new InetAddress[]{ IP_C, IP_D };

  private static URI uri1;
  private static URI uri2;
  private static List<URI> uris;

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setup() throws Exception {
    uri1 = new URI("http://example.com:80");
    uri2 = new URI("https://example.net:8080");
    uris = ImmutableList.of(uri1, uri2);
  }

  @Test
  public void testSupplierFactory() throws Exception {
    final DnsResolver resolver = mock(DnsResolver.class);
    when(resolver.resolve("example.com")).thenReturn(IPS_1);
    when(resolver.resolve("example.net")).thenReturn(IPS_2);
    final Supplier<List<URI>> uriSupplier = Suppliers.ofInstance(uris);
    final Supplier<List<Endpoint>> endpointSupplier = Endpoints.of(uriSupplier, resolver);
    final List<Endpoint> endpoints = endpointSupplier.get();

    assertThat(endpoints.size(), equalTo(4));
    assertThat(endpoints.get(0).getUri(), equalTo(uri1));
    assertThat(endpoints.get(0).getIp(), equalTo(IP_A));
    assertThat(endpoints.get(1).getUri(), equalTo(uri1));
    assertThat(endpoints.get(1).getIp(), equalTo(IP_B));
    assertThat(endpoints.get(2).getUri(), equalTo(uri2));
    assertThat(endpoints.get(2).getIp(), equalTo(IP_C));
    assertThat(endpoints.get(3).getUri(), equalTo(uri2));
    assertThat(endpoints.get(3).getIp(), equalTo(IP_D));
  }

  @Test
  public void testFactory() throws Exception {
    final DnsResolver resolver = mock(DnsResolver.class);
    when(resolver.resolve("example.com")).thenReturn(IPS_1);
    when(resolver.resolve("example.net")).thenReturn(IPS_2);
    final List<Endpoint> endpoints = Endpoints.of(uris, resolver);

    assertThat(endpoints.size(), equalTo(4));
    assertThat(endpoints.get(0).getUri(), equalTo(uri1));
    assertThat(endpoints.get(0).getIp(), equalTo(IP_A));
    assertThat(endpoints.get(1).getUri(), equalTo(uri1));
    assertThat(endpoints.get(1).getIp(), equalTo(IP_B));
    assertThat(endpoints.get(2).getUri(), equalTo(uri2));
    assertThat(endpoints.get(2).getIp(), equalTo(IP_C));
    assertThat(endpoints.get(3).getUri(), equalTo(uri2));
    assertThat(endpoints.get(3).getIp(), equalTo(IP_D));
  }

  @Test
  public void testUnableToResolve() throws Exception {
    final DnsResolver resolver = mock(DnsResolver.class);
    when(resolver.resolve("example.com")).thenThrow(new UnknownHostException());
    when(resolver.resolve("example.net")).thenThrow(new UnknownHostException());
    final List<Endpoint> endpoints = Endpoints.of(uris, resolver);

    assertThat(endpoints.size(), equalTo(0));
  }

  @Test
  public void testInvalidUri_NoScheme() throws Exception {
    final DnsResolver resolver = mock(DnsResolver.class);
    when(resolver.resolve("example.com")).thenReturn(IPS_1);
    exception.expect(IllegalArgumentException.class);
    Endpoints.of(ImmutableList.of(new URI(null, "example.com", null, null)), resolver);
  }

  @Test
  public void testInvalidUri_NoPort() throws Exception {
    final DnsResolver resolver = mock(DnsResolver.class);
    when(resolver.resolve("example.com")).thenReturn(IPS_1);
    exception.expect(IllegalArgumentException.class);
    Endpoints.of(ImmutableList.of(new URI("http", "example.com", null, null)), resolver);
  }
}
