/*-
 * -\-\-
 * Helios Services
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

package com.spotify.helios.servicescommon;

import com.google.common.base.Strings;
import com.spotify.helios.serviceregistration.NopServiceRegistrar;
import com.spotify.helios.serviceregistration.NopServiceRegistrarFactory;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.serviceregistration.ServiceRegistrarFactory;
import com.spotify.helios.serviceregistration.ServiceRegistrarLoader;
import com.spotify.helios.serviceregistration.ServiceRegistrarLoadingException;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads in the service registrar factory plugin (if specified) and returns an
 * appropriate {@link ServiceRegistrar}.
 */
public class ServiceRegistrars {

  private static final Logger log = LoggerFactory.getLogger(ServiceRegistrars.class);

  /**
   * Create a registrar. Attempts to load it from a plugin if path is not null or using the app
   * class loader otherwise. If no registrar plugin was found, returns a nop registrar.
   *
   * @param path    Path to plugin jar.
   * @param address The address of the registry the registrar should connect to.
   * @param domain  The domain that the registry should be managing.
   *
   * @return The ServiceRegistrar object.
   */
  public static ServiceRegistrar createServiceRegistrar(final Path path,
                                                        final String address,
                                                        final String domain) {
    // Get a registrar factory
    final ServiceRegistrarFactory factory;
    if (path == null) {
      factory = createFactory();
    } else {
      factory = createFactory(path);
    }

    // Create the registrar
    if (address != null) {
      log.info("Creating service registrar with address: {}", address);
      return factory.create(address);
    } else if (!Strings.isNullOrEmpty(domain)) {
      log.info("Creating service registrar for domain: {}", domain);

      // TODO: localhost:4999 is pretty specific to Spotify's registrar, this should be
      // handled in createForDomain there, rather than here.  Localhost should just pass through.
      return domain.equals("localhost")
             ? factory.create("tcp://localhost:4999")
             : factory.createForDomain(domain);
    } else {
      log.info("No address nor domain configured, not creating service registrar.");
      return new NopServiceRegistrar();
    }
  }

  /**
   * Get a registrar factory from a plugin.
   *
   * @param path The path to the plugin jar.
   *
   * @return The ServiceRegistrarFactory object.
   */
  private static ServiceRegistrarFactory createFactory(final Path path) {
    final ServiceRegistrarFactory factory;
    final Path absolutePath = path.toAbsolutePath();
    try {
      factory = ServiceRegistrarLoader.load(absolutePath);
      final String name = factory.getClass().getName();
      log.info("Loaded service registrar plugin: {} ({})", name, absolutePath);
    } catch (ServiceRegistrarLoadingException e) {
      throw new RuntimeException("Unable to load service registrar plugin: "
                                 + absolutePath, e);
    }
    return factory;
  }

  /**
   * Get a registrar factory from the application class loader.
   *
   * @return The ServiceRegistrarFactory object.
   */
  private static ServiceRegistrarFactory createFactory() {
    final ServiceRegistrarFactory factory;
    final ServiceRegistrarFactory installed;
    try {
      installed = ServiceRegistrarLoader.load();
    } catch (ServiceRegistrarLoadingException e) {
      throw new RuntimeException("Unable to load service registrar", e);
    }
    if (installed == null) {
      log.debug("No service registrar plugin configured");
      factory = new NopServiceRegistrarFactory();
    } else {
      factory = installed;
      final String name = factory.getClass().getName();
      log.info("Loaded installed service registrar: {}", name);
    }
    return factory;
  }
}
