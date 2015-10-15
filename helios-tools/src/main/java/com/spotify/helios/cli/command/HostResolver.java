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

package com.spotify.helios.cli.command;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.spotify.helios.authentication.HeliosAuthException;
import com.spotify.helios.client.HeliosClient;

import org.xbill.DNS.Name;
import org.xbill.DNS.ResolverConfig;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

class HostResolver {
  private static final Name[] EMPTY_PATH = new Name[]{};
  private final Set<String> allHosts;
  private final Name[] searchPath;

  HostResolver(final Set<String> allHosts,
               final Name[] searchPath) throws InterruptedException, ExecutionException {
    this.allHosts = allHosts;
    this.searchPath = searchPath;
  }

  static HostResolver create(HeliosClient client)
      throws InterruptedException, ExecutionException, HeliosAuthException {
    final ResolverConfig currentConfig = ResolverConfig.getCurrentConfig();
    final Name[] path;
    if (currentConfig != null) {
      final Name[] possiblePath = currentConfig.searchPath();
      if (possiblePath != null) {
        path = possiblePath;
      } else {
        path = EMPTY_PATH;
      }
    } else {
      path = EMPTY_PATH;
    }
    return new HostResolver(Sets.newHashSet(client.listHosts().get()), path);
  }

  private static class ScoredHost {
    private final String host;
    private final int score;

    public ScoredHost(String host, int score) {
      this.host = host;
      this.score = score;
    }
  }

  String resolveName(final String host) {
    // passed FQDN, all set
    if (allHosts.contains(host)) {
      return host;
    }

    final List<String> matches = findPrefixMatches(host);
    if (matches.isEmpty()) {
      return host;  // no pfx matches, let it fail upstream
    }

    if (matches.size() == 1) {
      return matches.iterator().next();
    }

    final List<ScoredHost> scored = scoreMatches(matches);
    final List<ScoredHost> sorted = sortScoredHosts(scored);
    final List<String> minScoreHosts = findMatchesWithLowestScore(sorted);

    if (minScoreHosts.size() > 1) {
      // ambiguous, just return it and let it fail upstream
      return host;
    }

    return minScoreHosts.get(0);
  }

  public List<String> getSortedMatches(final String hostName) {
    final List<String> matches = findPrefixMatches(hostName);
    final List<ScoredHost> scored = scoreMatches(matches);
    final List<ScoredHost> sorted = sortScoredHosts(scored);
    final ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (final ScoredHost host : sorted) {
      builder.add(host.host);
    }
    return builder.build();
  }

  private List<ScoredHost> sortScoredHosts(List<ScoredHost> scored) {
    final List<ScoredHost> copy = Lists.newArrayList(scored);
    Collections.sort(copy, new Comparator<ScoredHost>() {
      @Override
      public int compare(ScoredHost o1, ScoredHost o2) {
        return o1.score - o2.score;
      }
    });
    return copy;
  }

  private List<String> findPrefixMatches(final String pfx) {
    final ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (final String name : allHosts) {
      if (name.startsWith(pfx)) {
        builder.add(name);
      }
    }
    return builder.build();
  }

  // Assumes sorted input in scored
  private List<String> findMatchesWithLowestScore(final List<ScoredHost> scored) {
    final int minScore = scored.get(0).score;
    final ImmutableList.Builder<String> minScoreHosts = ImmutableList.builder();
    for (ScoredHost score : scored) {
      if (score.score == minScore) {
        minScoreHosts.add(score.host);
      }
    }
    return minScoreHosts.build();
  }

  // Score matches based upon the position in the dns search domain that matches the result
  private List<ScoredHost> scoreMatches(final List<String> results) {
    final ImmutableList.Builder<ScoredHost> scored = ImmutableList.builder();
    for (final String name : results) {
      int score = Integer.MAX_VALUE;
      for (int i = 0; i < searchPath.length; i++) {
        if (name.endsWith(searchPath[i].toString())) {
          if (i < score) {
            score = i;
          }
        }
      }
      scored.add(new ScoredHost(name, score));
    }
    return scored.build();
  }
}
