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

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;

import org.apache.http.conn.DnsResolver;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetAddress;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EndpointIteratorTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private static final DnsResolver resolver = mock(DnsResolver.class);
  private static final InetAddress[] ips1 = new InetAddress[] {
      InetAddresses.forString("1.2.3.4"),
      InetAddresses.forString("2.3.4.5"),
  };
  private static final InetAddress[] ips2 = new InetAddress[] {
    InetAddresses.forString("3.4.5.6"),
    InetAddresses.forString("4.5.6.7"),
  };
  private static List<Endpoint> endpoints;

  @Before
  public void setup() throws Exception {
    when(resolver.resolve("example.com")).thenReturn(ips1);
    when(resolver.resolve("example.net")).thenReturn(ips2);
    final URI uri1 = new URI("http://example.com");
    final URI uri2 = new URI("https://example.net");
    final List<URI> uris = ImmutableList.of(uri1, uri2);
    endpoints = Endpoints.of(uris, resolver);
  }

  @Test
  public void test() throws Exception {
    final EndpointIterator iterator = EndpointIterator.of(endpoints);

    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasNextIp());
    assertThat(iterator.nextIp(), equalTo(ips1[0]));
    assertThat(iterator.nextIp(), equalTo(ips1[1]));
    exception.expect(NoSuchElementException.class);
    iterator.nextIp();

    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasNextIp());
    assertThat(iterator.nextIp(), equalTo(ips2[0]));
    assertThat(iterator.nextIp(), equalTo(ips2[1]));
    exception.expect(NoSuchElementException.class);
    iterator.nextIp();
    exception.expect(NoSuchElementException.class);
    iterator.next();

    assertFalse(iterator.hasNext());
    assertFalse(iterator.hasNextIp());
  }

  @Test
  public void testEmptyIterator() throws Exception {
    final EndpointIterator iterator = EndpointIterator.of(Collections.<Endpoint>emptyList());

    assertFalse(iterator.hasNext());
    assertFalse(iterator.hasNextIp());
    exception.expect(NoSuchElementException.class);
    iterator.next();
    exception.expect(NoSuchElementException.class);
    iterator.nextIp();
  }
}
