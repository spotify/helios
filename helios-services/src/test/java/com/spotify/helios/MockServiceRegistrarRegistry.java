/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios;

import com.google.common.collect.Maps;

import com.spotify.helios.serviceregistration.ServiceRegistrar;

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
