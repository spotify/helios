/*-
 * -\-\-
 * Helios Service Registration
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
   *
   * @return A registrar.
   */
  ServiceRegistrar create(String address);

  /**
   * Create a service registrar connected to a registry managing a specific domain. The domain
   * format and semantics are implementation dependent. This method is typically used when the
   * registry in turn is resolved using some service discovery mechanism, e.g. DNS SRV queries.
   *
   * @param domain The domain that the registry should be managing.
   *
   * @return A registrar.
   */
  ServiceRegistrar createForDomain(String domain);
}
