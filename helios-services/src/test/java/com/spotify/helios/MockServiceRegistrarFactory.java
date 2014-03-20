/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios;

import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.serviceregistration.ServiceRegistrarFactory;

import static org.junit.Assert.assertNotNull;

public class MockServiceRegistrarFactory implements ServiceRegistrarFactory {

  @Override
  public ServiceRegistrar create(final String address) {
    final ServiceRegistrar registrar = MockServiceRegistrarRegistry.get(address);
    assertNotNull(registrar);
    return registrar;
  }

  @Override
  public ServiceRegistrar createForDomain(final String domain) {
    throw new UnsupportedOperationException();
  }
}
