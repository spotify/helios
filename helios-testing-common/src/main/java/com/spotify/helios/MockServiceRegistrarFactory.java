/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios;

import static org.junit.Assert.assertNotNull;

import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.serviceregistration.ServiceRegistrarFactory;

public class MockServiceRegistrarFactory implements ServiceRegistrarFactory {

  @Override
  public ServiceRegistrar create(final String address) {
    final ServiceRegistrar registrar = MockServiceRegistrarRegistry.get(address);
    assertNotNull(registrar);
    return registrar;
  }

  @Override
  public ServiceRegistrar createForDomain(final String domain) {
    throw new UnsupportedOperationException();
  }
}
