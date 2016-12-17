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

package com.spotify.helios.master;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.HostSelector;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HostMatcherTest {

  private final Map<String, Map<String, String>> hosts = ImmutableMap.of(
      "foo-a1", ImmutableMap.of("role", "foo"),
      "foo-a2", ImmutableMap.of("role", "foo", "special", "yes"),
      "bar-a1", ImmutableMap.of("role", "bar", "pool", "a"),
      "bar-b1", ImmutableMap.of("role", "bar", "pool", "b"),
      "bar-c1", ImmutableMap.of("role", "bar", "pool", "c")
  );

  private final HostMatcher matcher = new HostMatcher(hosts);

  private static DeploymentGroup group(String... selectorStrings) {
    final List<HostSelector> selectors = new ArrayList<>();
    for (final String selectorString : selectorStrings) {
      final HostSelector selector = HostSelector.parse(selectorString);
      if (selector == null) {
        throw new IllegalArgumentException("bad selector: " + selectorString);
      }
      selectors.add(selector);
    }

    return DeploymentGroup.newBuilder()
        .setHostSelectors(selectors)
        .build();
  }

  @Test
  public void testHostMatcher() {
    assertThat(matcher.getMatchingHosts(group("role=foo")), contains("foo-a1", "foo-a2"));

    assertThat(matcher.getMatchingHosts(group("role=foo", "special=yes")), contains("foo-a2"));
    // does not match foo-a1 because it has no 'special' label
    assertThat(matcher.getMatchingHosts(group("role=foo", "special!=yes")), empty());

    assertThat(matcher.getMatchingHosts(group("pool=a")), contains("bar-a1"));
    assertThat(matcher.getMatchingHosts(group("pool=a", "role=bar")), contains("bar-a1"));

    assertThat(matcher.getMatchingHosts(group("pool=b")), contains("bar-b1"));
    assertThat(matcher.getMatchingHosts(group("pool=b", "role=bar")), contains("bar-b1"));

    assertThat(matcher.getMatchingHosts(group("pool=c")), contains("bar-c1"));
    assertThat(matcher.getMatchingHosts(group("pool=c", "role=bar")), contains("bar-c1"));

    // groups where some selectors match hosts but others do not, should return no matches
    assertThat(matcher.getMatchingHosts(group("pool=c", "role=awesome")), empty());
    assertThat(matcher.getMatchingHosts(group("special=yes", "role=awesome")), empty());
  }


  @Test
  public void testDeploymentGroupWithNoSelectors() {
    final DeploymentGroup deploymentGroup = group();
    assertThat(matcher.getMatchingHosts(deploymentGroup), empty());
  }

}
