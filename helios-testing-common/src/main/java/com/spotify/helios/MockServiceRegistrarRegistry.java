/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios;

import com.spotify.helios.serviceregistration.ServiceRegistrar;

import com.google.common.collect.Maps;

import java.util.Map;

public class MockServiceRegistrarRegistry {
  private static final Map<String, ServiceRegistrar> registrars = Maps.newConcurrentMap();

  public static void set(final String address, final ServiceRegistrar registrar) {
    registrars.put(address, registrar);
  }

  public static ServiceRegistrar get(final String address) {
    return registrars.get(address);
  }

  public static void remove(final String address) {
    registrars.remove(address);
  }
}
