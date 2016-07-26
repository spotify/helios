/*
 * Copyright (c) 2016 Spotify AB.
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

package com.spotify.helios.master.resources;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.master.MasterModel;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HostsResourceTest {

  private static final List<String> NO_LABELS_ARG = Collections.emptyList();

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

    final HostStatus.Builder statusBuilder = HostStatus.newBuilder()
        .setStatus(HostStatus.Status.UP)
        .setJobs(Collections.emptyMap())
        .setStatuses(Collections.emptyMap());

    int i = 1;
    for (final String host : hosts) {
      final Map<String, String> labels = new HashMap<>();
      labels.put("site", "foo");
      labels.put("index", String.valueOf(i++));

      final HostStatus hostStatus = statusBuilder
          .setLabels(labels)
          .build();

      when(model.getHostStatus(host)).thenReturn(hostStatus);
    }
  }

  @Test
  public void listHosts() {
    assertThat(resource.list(null, NO_LABELS_ARG), equalTo(hosts));
  }

  @Test
  public void listHostsNameFilter() {
    assertThat(resource.list("foo.example", NO_LABELS_ARG), equalTo(hosts));
    assertThat(resource.list("host1", NO_LABELS_ARG), contains("host1.foo.example.com"));
    assertThat(resource.list("host5", NO_LABELS_ARG), empty());
  }

  @Test
  public void listHostsLabelFilter() {
    assertThat(resource.list(null, ImmutableList.of("site=foo")), equalTo(hosts));

    assertThat(resource.list(null, ImmutableList.of("site=bar")), empty());
    assertThat(resource.list(null, ImmutableList.of("site!=foo")), empty());

    assertThat(resource.list(null, ImmutableList.of("index in (1,2)")),
               contains("host1.foo.example.com", "host2.foo.example.com"));

    assertThat(resource.list(null, ImmutableList.of("site=foo", "index in (1,2)")),
               contains("host1.foo.example.com", "host2.foo.example.com"));
  }

  /** Test behavior when both a name pattern and labels list is specified */
  @Test
  public void listHostsNameAndLabelFilter() {
    assertThat(resource.list("foo.example.com", ImmutableList.of("site=foo")), equalTo(hosts));

    assertThat(resource.list("host3", ImmutableList.of("index =2")), empty());

    assertThat(resource.list("host3", ImmutableList.of("index!=2")),
               contains("host3.foo.example.com"));
  }
}
