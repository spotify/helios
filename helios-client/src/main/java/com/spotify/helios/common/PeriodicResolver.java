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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper around {@link Resolver} that periodically refreshes SRV records.
 */
public class PeriodicResolver implements Supplier<List<URI>> {

  private List<URI> endpoints;

  private PeriodicResolver(final String srvName,
                           final String domain,
                           final Resolver resolver,
                           final ScheduledExecutorService executorService) {
    endpoints = resolver.resolve(srvName, domain);
    executorService.scheduleWithFixedDelay(() ->
        endpoints = resolver.resolve(srvName, domain), 0, 1, TimeUnit.MINUTES);
  }

  public static PeriodicResolver create(final String srvName,
                                        final String domain,
                                        final ScheduledExecutorService executorService) {
    return new PeriodicResolver(srvName, domain, new Resolver(), executorService);
  }

  @VisibleForTesting
  public static PeriodicResolver create(final String srvName,
                                        final String domain,
                                        final Resolver resolver,
                                        final ScheduledExecutorService executorService) {
    return new PeriodicResolver(srvName, domain, resolver, executorService);
  }

  @Override
  public List<URI> get() {
    return endpoints;
  }
}
