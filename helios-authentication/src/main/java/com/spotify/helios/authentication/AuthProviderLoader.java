/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package com.spotify.helios.authentication;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedInteger;

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.InjectableProvider;

import org.slf4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import javax.ws.rs.WebApplicationException;

import io.dropwizard.auth.Authenticator;

import static java.util.Arrays.asList;

/**
 * Helpers for loading {@link AuthProviderFactory} instances.
 */
public class AuthProviderLoader {

  private static final List<Package> PROVIDED = asList(
      Logger.class.getPackage(),
      AuthProviderLoader.class.getPackage(),
      Authenticator.class.getPackage(),
      InjectableProvider.class.getPackage(),
      Preconditions.class.getPackage(),
      UnsignedInteger.class.getPackage(),
      Parameter.class.getPackage(),
      ComponentContext.class.getPackage(),
      AbstractHttpContextInjectable.class.getPackage(),
      WebApplicationException.class.getPackage(),
      HttpContext.class.getPackage(),
      BaseEncoding.class.getPackage(),
      Hashing.class.getPackage()
  );

  private static final ClassLoader CURRENT = AuthProviderLoader.class.getClassLoader();

  /**
   * Load a {@link AuthProviderFactory} using the current class loader.
   *
   * @return A {@link AuthProviderFactory}, null if none could be found.
   * @throws AuthProviderLoadingException if loading failed.
   */
  public static AuthProviderFactory load() throws AuthProviderLoadingException {
    return load("classpath", CURRENT);
  }

  /**
   * Load a {@link AuthProviderFactory} from a plugin jar file with a parent class loader that
   * will not load classes from the jvm classpath. Any dependencies of the plugin must be included
   * in the plugin jar.
   *
   * @param plugin The plugin jar file to load.
   * @return A {@link AuthProviderFactory}, null if none could be found.
   * @throws AuthProviderLoadingException if loading failed.
   */
  public static AuthProviderFactory load(final Path plugin)
      throws AuthProviderLoadingException {
    return load(plugin, CURRENT, extensionClassLoader(CURRENT));
  }

  /**
   * Load a {@link AuthProviderFactory} from a plugin jar file with a specified parent class
   * loader and a list of exposed classes.
   *
   * @param plugin      The plugin jar file to load.
   * @param environment The class loader to use for providing plugin interface dependencies.
   * @param parent      The parent class loader to assign to the class loader of the jar.
   * @return A {@link AuthProviderFactory}, null if none could be found.
   * @throws AuthProviderLoadingException if loading failed.
   */
  public static AuthProviderFactory load(final Path plugin,
                                               final ClassLoader environment,
                                               final ClassLoader parent)
      throws AuthProviderLoadingException {
    return load("plugin jar file: " + plugin, pluginClassLoader(plugin, environment, parent));
  }

  /**
   * Load a {@link AuthProviderFactory} using a class loader.
   *
   * @param source      The source of the class loader.
   * @param classLoader The class loader to load from.
   * @return A {@link AuthProviderFactory}, null if none could be found.
   * @throws AuthProviderLoadingException if loading failed.
   */
  public static AuthProviderFactory load(final String source, final ClassLoader classLoader)
      throws AuthProviderLoadingException {
    final ServiceLoader<AuthProviderFactory> loader;
    try {
      loader = ServiceLoader.load(AuthProviderFactory.class, classLoader);
    } catch (ServiceConfigurationError e) {
      throw new AuthProviderLoadingException(
          "Failed to load service registrar from " + source, e);
    }
    final Iterator<AuthProviderFactory> iterator = loader.iterator();
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
    return new URLClassLoader(new URL[]{url}, providedClassLoader);
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
