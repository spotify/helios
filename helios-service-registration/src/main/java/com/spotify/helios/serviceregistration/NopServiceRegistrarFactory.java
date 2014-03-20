/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.serviceregistration;

/**
 * A factory for nop service registrars.
 */
public class NopServiceRegistrarFactory implements ServiceRegistrarFactory {

  @Override
  public ServiceRegistrar create(final String address) {
    return new NopServiceRegistrar();
  }

  @Override
  public ServiceRegistrar createForDomain(final String domain) {
    return new NopServiceRegistrar();
  }
}
