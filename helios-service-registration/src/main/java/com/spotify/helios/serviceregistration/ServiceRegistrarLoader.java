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

import static java.util.Arrays.asList;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import org.slf4j.Logger;

/**
 * Helpers for loading {@link ServiceRegistrarFactory} instances.
 */
public class ServiceRegistrarLoader {

  private static final List<Package> PROVIDED = asList(Logger.class.getPackage(),
      ServiceRegistrarLoader.class.getPackage());

  private static final ClassLoader CURRENT = ServiceRegistrarLoader.class.getClassLoader();

  /**
   * Load a {@link ServiceRegistrarFactory} using the current class loader.
   *
   * @return A {@link ServiceRegistrarFactory}, null if none could be found.
   *
   * @throws ServiceRegistrarLoadingException if loading failed.
   */
  public static ServiceRegistrarFactory load() throws ServiceRegistrarLoadingException {
    return load("classpath", CURRENT);
  }

  /**
   * Load a {@link ServiceRegistrarFactory} from a plugin jar file with a parent class loader that
   * will not load classes from the jvm classpath. Any dependencies of the plugin must be included
   * in the plugin jar.
   *
   * @param plugin The plugin jar file to load.
   *
   * @return A {@link ServiceRegistrarFactory}, null if none could be found.
   *
   * @throws ServiceRegistrarLoadingException if loading failed.
   */
  public static ServiceRegistrarFactory load(final Path plugin)
      throws ServiceRegistrarLoadingException {
    return load(plugin, CURRENT, extensionClassLoader(CURRENT));
  }

  /**
   * Load a {@link ServiceRegistrarFactory} from a plugin jar file with a specified parent class
   * loader and a list of exposed classes.
   *
   * @param plugin      The plugin jar file to load.
   * @param environment The class loader to use for providing plugin interface dependencies.
   * @param parent      The parent class loader to assign to the class loader of the jar.
   *
   * @return A {@link ServiceRegistrarFactory}, null if none could be found.
   *
   * @throws ServiceRegistrarLoadingException if loading failed.
   */
  public static ServiceRegistrarFactory load(final Path plugin,
                                             final ClassLoader environment,
                                             final ClassLoader parent)
      throws ServiceRegistrarLoadingException {
    return load("plugin jar file: " + plugin, pluginClassLoader(plugin, environment, parent));
  }

  /**
   * Load a {@link ServiceRegistrarFactory} using a class loader.
   *
   * @param source      The source of the class loader.
   * @param classLoader The class loader to load from.
   *
   * @return A {@link ServiceRegistrarFactory}, null if none could be found.
   *
   * @throws ServiceRegistrarLoadingException if loading failed.
   */
  public static ServiceRegistrarFactory load(final String source, final ClassLoader classLoader)
      throws ServiceRegistrarLoadingException {
    final ServiceLoader<ServiceRegistrarFactory> loader;
    try {
      loader = ServiceLoader.load(ServiceRegistrarFactory.class, classLoader);
    } catch (ServiceConfigurationError e) {
      throw new ServiceRegistrarLoadingException(
          "Failed to load service registrar from " + source, e);
    }
    final Iterator<ServiceRegistrarFactory> iterator = loader.iterator();
    if (iterator.hasNext()) {
      return iterator.next();
    } else {
      return null;
    }
  }

  /**
   * Attempt to get the extension class loader, which can only load jdk classes and classes from
   * jvm
   * extensions. Adapted from {@link ServiceLoader#loadInstalled(Class)}
   */
  private static ClassLoader extensionClassLoader(final ClassLoader environment) {
    ClassLoader cl = environment;
    ClassLoader prev = null;
    while (cl != null) {
      prev = cl;
      cl = cl.getParent();
    }
    return prev;
  }

  /**
   * Create a class loader for a plugin jar.
   */
  private static ClassLoader pluginClassLoader(final Path plugin,
                                               final ClassLoader environment,
                                               final ClassLoader parent) {
    final URL url;
    try {
      url = plugin.toFile().toURI().toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException("Failed to load plugin jar " + plugin, e);
    }
    final ClassLoader providedClassLoader = new FilteringClassLoader(PROVIDED, environment, parent);
    return new URLClassLoader(new URL[]{ url }, providedClassLoader);
  }

  /**
   * A class loader that exposes a specific list of packages from another class loader.
   */
  private static class FilteringClassLoader extends ClassLoader {

    private final List<Package> packages;
    private final ClassLoader environment;

    public FilteringClassLoader(final List<Package> packages,
                                final ClassLoader environment,
                                final ClassLoader parent) {
      super(parent);
      this.packages = packages;
      this.environment = environment;
    }

    @Override
    protected Class<?> findClass(final String name) throws ClassNotFoundException {
      for (final Package pkg : packages) {
        if (name.startsWith(pkg.getName())) {
          return environment.loadClass(name);
        }
      }
      return super.findClass(name);
    }
  }
}
