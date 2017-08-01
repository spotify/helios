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
 * A registrar for service endpoints.
 */
public interface ServiceRegistrar extends AutoCloseable {

  /**
   * Asynchronously register a set of service endpoints with a registry. Does not block.
   *
   * @param registration The service endpoints.
   *
   * @return A handle that can be used to unregister using
   *         {@link #unregister(ServiceRegistrationHandle)}.
   */
  ServiceRegistrationHandle register(ServiceRegistration registration);

  /**
   * Unregister a set of service endpoints previously registered.
   *
   * @param handle A handle returned by {@link #register(ServiceRegistration)}.
   */
  void unregister(ServiceRegistrationHandle handle);

  /**
   * Close this registrar, possibly unregistering all registered service endpoints.
   */
  void close();
}
