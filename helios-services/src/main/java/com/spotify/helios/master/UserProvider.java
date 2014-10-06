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

package com.spotify.helios.master;

import com.spotify.helios.master.resources.RequestUser;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;

import java.lang.reflect.Type;
import java.util.List;

import javax.ws.rs.ext.Provider;

@Provider
public class UserProvider extends AbstractHttpContextInjectable<String>
   implements InjectableProvider<RequestUser, Type> {

  @Override
  public String getValue(HttpContext arg0) {
    final List<String> usernames = arg0.getRequest().getQueryParameters().get("user");
    if (usernames.isEmpty()) {
      return null;
    }
    return usernames.get(0);
  }

  @Override
  public Injectable<?> getInjectable(ComponentContext ctx, RequestUser ruAnnotation, Type type) {
    if (type.equals(String.class)) {
      return this;
    }
    return null;
  }

  @Override
  public ComponentScope getScope() {
    return ComponentScope.PerRequest;
  }
}
