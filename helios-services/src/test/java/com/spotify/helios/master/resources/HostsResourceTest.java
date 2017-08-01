/*-
 * -\-\-
 * Helios Services
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

package com.spotify.helios.master.resources;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.spotify.helios.master.MasterModel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HostsResourceTest {

  private static final List<String> NO_SELECTOR_ARG = Collections.emptyList();

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private final MasterModel model = mock(MasterModel.class);
  private final HostsResource resource = new HostsResource(model);

  private final ImmutableList<String> hosts = ImmutableList.of(
      "host1.foo.example.com",
      "host2.foo.example.com",
      "host3.foo.example.com",
      "host4.foo.example.com"
  );

  @Before
  public void setUp() {
    when(model.listHosts()).thenReturn(hosts);

    int idx = 1;
    for (final String host : hosts) {
      final Map<String, String> labels = new HashMap<>();
      labels.put("site", "foo");
      labels.put("index", String.valueOf(idx++));

      when(model.getHostLabels(host)).thenReturn(labels);
    }
  }

  @Test
  public void listHosts() {
    assertThat(resource.list(null, NO_SELECTOR_ARG), equalTo(hosts));
  }

  @Test
  public void listHostsNameFilter() {
    when(model.listHosts("foo.example")).thenReturn(hosts);
    assertThat(resource.list("foo.example", NO_SELECTOR_ARG), equalTo(hosts));

    when(model.listHosts("host1")).thenReturn(ImmutableList.of("host1.foo.example.com"));
    assertThat(resource.list("host1", NO_SELECTOR_ARG), contains("host1.foo.example.com"));

    when(model.listHosts("host5")).thenReturn(ImmutableList.of());
    assertThat(resource.list("host5", NO_SELECTOR_ARG), empty());
  }

  @Test
  public void listHostsSelectorFilter() {
    assertThat(resource.list(null, ImmutableList.of("site=foo")), equalTo(hosts));

    assertThat(resource.list(null, ImmutableList.of("site=bar")), empty());
    assertThat(resource.list(null, ImmutableList.of("site!=foo")), empty());

    assertThat(resource.list(null, ImmutableList.of("index in (1,2)")),
        contains("host1.foo.example.com", "host2.foo.example.com"));

    assertThat(resource.list(null, ImmutableList.of("site=foo", "index in (1,2)")),
        contains("host1.foo.example.com", "host2.foo.example.com"));
  }

  @Test
  public void listHostsSelectorFilterMissingStatus() {
    when(model.getHostLabels(hosts.get(0))).thenReturn(emptyMap());
    assertThat(resource.list(null, ImmutableList.of("site=foo")),
        equalTo(hosts.subList(1, hosts.size())));

    assertThat(resource.list(null, ImmutableList.of("site=bar")), empty());
    assertThat(resource.list(null, ImmutableList.of("site!=foo")), empty());

    assertThat(resource.list(null, ImmutableList.of("index in (1,2)")),
        contains("host2.foo.example.com"));

    assertThat(resource.list(null, ImmutableList.of("site=foo", "index in (1,2)")),
        contains("host2.foo.example.com"));
  }

  // Test behavior when both a name pattern and selector list is specified.
  @Test
  public void listHostsNameAndSelectorFilter() {
    when(model.listHosts("foo.example.com")).thenReturn(hosts);
    assertThat(resource.list("foo.example.com", ImmutableList.of("site=foo")), equalTo(hosts));

    when(model.listHosts("host3")).thenReturn(ImmutableList.of("host3.foo.example.com"));
    assertThat(resource.list("host3", ImmutableList.of("index =2")), empty());

    assertThat(resource.list("host3", ImmutableList.of("index!=2")),
        contains("host3.foo.example.com"));
  }

  @Test
  public void listHostsInvalidHostSelectorSyntax() {
    exception.expect(WebApplicationException.class);
    exception.expect(hasStatus(Response.Status.BAD_REQUEST));

    resource.list(null, ImmutableList.of("foo <@> bar"));
  }

  private static Matcher<WebApplicationException> hasStatus(final Response.Status status) {
    final int statusCode = status.getStatusCode();
    final String msg = "WebApplicationException with response.statusCode=" + statusCode;
    return new CustomTypeSafeMatcher<WebApplicationException>(msg) {
      @Override
      protected boolean matchesSafely(final WebApplicationException item) {
        return item.getResponse() != null && item.getResponse().getStatus() == statusCode;
      }
    };
  }
}
