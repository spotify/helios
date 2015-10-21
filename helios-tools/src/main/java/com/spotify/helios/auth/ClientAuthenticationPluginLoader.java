/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.auth;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.List;
import java.util.ServiceLoader;

public class ClientAuthenticationPluginLoader {

  public static List<ClientAuthenticationPlugin> loadAll() {
    return loadAll(null);
  }

  public static List<ClientAuthenticationPlugin> loadAll(final Path pluginPath) {
    final ServiceLoader<ClientAuthenticationPlugin> loader =
        serviceLoaderForPath(pluginPath);
    final List<ClientAuthenticationPlugin> plugins = Lists.newArrayList();
    Iterators.addAll(plugins, loader.iterator());
    return ImmutableList.copyOf(plugins);
  }

  private static ServiceLoader<ClientAuthenticationPlugin> serviceLoaderForPath(
      final Path pluginPath) {
    final ClassLoader classLoader;
    if (pluginPath == null) {
      // default loader = this one
      classLoader = Thread.currentThread().getContextClassLoader();
    } else {
      // load from plugin path *only*
      Preconditions.checkArgument(pluginPath.toFile().canRead(),
                                  "Plugin path " + pluginPath
                                  + " does not exist or is not readable");
      classLoader = pluginClassLoader(pluginPath);
    }

    return ServiceLoader.load(ClientAuthenticationPlugin.class, classLoader);
  }

  /**
   * Create a class loader for a plugin jar.
   */
  private static ClassLoader pluginClassLoader(final Path plugin) {
    try {
      final URL url = plugin.toFile().toURI().toURL();
      return new URLClassLoader(new URL[]{url});
    } catch (MalformedURLException e) {
      throw new RuntimeException("Failed to load plugin jar " + plugin, e);
    }
  }
}
