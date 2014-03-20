/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.serviceregistration;

/**
 * A factory for {@link ServiceRegistrar} instances, and the entry point for service registrar
 * plugins. {@link ServiceRegistrarLoader} loads plugins by using {@link java.util.ServiceLoader}
 * to
 * look up {@link ServiceRegistrarFactory} from jar files and class loaders.
 */
public interface ServiceRegistrarFactory {

  /**
   * Create a service registrar connected to a registry at a specific address. The address format
   * and semantics are implementation dependent.
   *
   * @param address The address of the registry the registrar should connect to.
   * @return A registrar.
   */
  ServiceRegistrar create(String address);

  /**
   * Create a service registrar connected to a registry managing a specific domain. The domain
   * format and semantics are implementation dependent. This method is typically used when the
   * registry in turn is resolved using some service discovery mechanism, e.g. DNS SRV queries.
   *
   * @param domain The domain that the registry should be managing.
   */
  ServiceRegistrar createForDomain(String domain);
}
