/*-
 * -\-\-
 * Helios Tools
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

package com.spotify.helios.cli.command;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.xbill.DNS.Name;

public class HostResolverTest {

  private static final Name[] SEARCH_PATH = new Name[]{ Name.fromConstantString("bar.com"),
                                                        Name.fromConstantString("foo.com") };
  private static final ImmutableSet<String> ALL_HOSTS = ImmutableSet.of(
      "host1.bar.com", "host1.foo.com");

  @Test
  public void test() throws Exception {
    final HostResolver resolver = new HostResolver(ALL_HOSTS, new Name[]{});
    // fully spec'd that's in allHosts
    assertEquals("host1.bar.com", resolver.resolveName("host1.bar.com"));

    // fully spec'd not in allHosts
    assertEquals("whee.bar.com", resolver.resolveName("whee.bar.com"));

    // can disambiguate on unique prefix
    assertEquals("host1.bar.com", resolver.resolveName("host1.bar"));

    // can disambiguate on unique prefix
    assertEquals("host1.bar.com", resolver.resolveName("host1.b"));

    // falls through when can't disambiguate (here, w/o search paths)
    assertEquals("host1", resolver.resolveName("host1"));
  }

  @Test
  public void testWithSearchPathDisambiguation() throws Exception {
    final HostResolver resolver = new HostResolver(ALL_HOSTS, SEARCH_PATH);

    assertEquals("host1.bar.com", resolver.resolveName("host1"));
    assertEquals("host1.bar.com", resolver.resolveName("ho"));
  }

  @Test
  public void testAmbiguousWithSearchPathDisambiguation() throws Exception {
    final HostResolver resolver = new HostResolver(
        ImmutableSet.of("host1.foo.com", "host2.foo.com"),
        SEARCH_PATH);

    // Even with path disambiguation, we can't disambiguate
    assertEquals("ho", resolver.resolveName("ho"));
  }
}
