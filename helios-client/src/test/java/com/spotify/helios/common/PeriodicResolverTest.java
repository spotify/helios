/*-
 * -\-\-
 * Helios Client
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

package com.spotify.helios.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.util.concurrent.Executors;
import org.hamcrest.Matchers;
import org.junit.Test;

public class PeriodicResolverTest {

  private final Resolver resolver = mock(Resolver.class);

  @Test
  public void testGet() {
    final URI uri = URI.create("https://foo.bar.com");
    when(resolver.resolve("foo", "bar")).thenReturn(ImmutableList.of(uri));
    final PeriodicResolver sut = PeriodicResolver.create("foo", "bar", resolver,
        Executors.newSingleThreadScheduledExecutor());
    assertThat(sut.get(), Matchers.contains(uri));
  }
}