/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.serviceregistration;

/**
 * A nop service registrar that does nothing. Useful as an alternative to null.
 */
public class NopServiceRegistrar implements ServiceRegistrar {

  @Override
  public ServiceRegistrationHandle register(final ServiceRegistration registration) {
    return new NopServiceRegistrationHandle();
  }

  @Override
  public void unregister(final ServiceRegistrationHandle handle) {
  }

  @Override
  public void close() {
  }
}
