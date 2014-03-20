/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.serviceregistration;

/**
 * A registrar for service endpoints.
 */
public interface ServiceRegistrar extends AutoCloseable {

  /**
   * Asynchronously register a set of service endpoints with a registry. Does not block.
   *
   * @param registration The service endpoints.
   * @return A handle that can be used to unregister using {@link #unregister(ServiceRegistrationHandle)}.
   */
  ServiceRegistrationHandle register(ServiceRegistration registration);

  /**
   * Unregister a set of service endpoints previously registered.
   * @param handle A handle returned by {@link #register(ServiceRegistration)}.
   */
  void unregister(ServiceRegistrationHandle handle);

  /**
   * Close this registrar, possibly unregistering all registered service endpoints.
   */
  void close();
}
