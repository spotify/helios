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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyList;

import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.HostSelector;
import com.spotify.helios.rollingupdate.AlphaNumericComparator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Filters hosts based on their labels according to a List of HostSelectors.
 */
public class HostMatcher {

  private static final Logger log = LoggerFactory.getLogger(HostMatcher.class);

  private final Map<String, Map<String, String>> hostsAndLabels;

  public HostMatcher(final Map<String, Map<String, String>> hostsAndLabels) {
    this.hostsAndLabels = ImmutableMap.copyOf(checkNotNull(hostsAndLabels, "hostsAndLabels"));
  }

  public List<String> getMatchingHosts(final DeploymentGroup deploymentGroup) {
    final List<HostSelector> selectors = deploymentGroup.getHostSelectors();
    if (selectors == null || selectors.isEmpty()) {
      log.error("skipping deployment group with no host selectors: " + deploymentGroup.getName());
      return emptyList();
    }

    return getMatchingHosts(selectors);
  }

  public List<String> getMatchingHosts(final List<HostSelector> selectors) {

    final List<String> matchingHosts = Lists.newArrayList();

    for (final Map.Entry<String, Map<String, String>> entry : hostsAndLabels.entrySet()) {
      final String host = entry.getKey();
      final Map<String, String> hostLabels = entry.getValue();
      if (hostLabels == null) {
        continue;
      }

      // every hostSelector in the group has to have a match in this host.
      // a match meaning the host has a label for that key and the value matches
      final boolean match = selectors.stream()
          .allMatch(selector -> hostLabels.containsKey(selector.getLabel())
                                && selector.matches(hostLabels.get(selector.getLabel())));

      if (match) {
        matchingHosts.add(host);
      }
    }

    Collections.sort(matchingHosts, new AlphaNumericComparator(Locale.ENGLISH));
    return ImmutableList.copyOf(matchingHosts);
  }
}
