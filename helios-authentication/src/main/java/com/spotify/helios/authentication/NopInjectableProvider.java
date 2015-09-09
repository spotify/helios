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

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;

import io.dropwizard.auth.Auth;
import io.dropwizard.auth.Authenticator;

/**
 * A nop authenticator that does nothing. Useful as an alternative to null.
 */
public class NopInjectableProvider implements InjectableProvider<Auth, Parameter> {

  private final Authenticator authenticator;

  public NopInjectableProvider() {
    this.authenticator = new NopAuthenticator();
  }

  @Override
  public ComponentScope getScope() {
    return ComponentScope.PerRequest;
  }

  @Override
  public Injectable<?> getInjectable(ComponentContext ic, Auth a, Parameter c) {
    return new NopInjectable<>(authenticator, a.required());
  }

  private static class NopInjectable<T> extends AbstractHttpContextInjectable<T> {
    @SuppressWarnings("UnusedParameters")
    private NopInjectable(final Authenticator authenticator, boolean required) {
    }

    @Override
    public T getValue(HttpContext c) {
      return null;
    }
  }
}
