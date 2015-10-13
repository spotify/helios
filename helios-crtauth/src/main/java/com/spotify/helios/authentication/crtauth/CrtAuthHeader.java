/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.helios.authentication.crtauth;

import com.spotify.helios.authentication.AuthHeader;

public class CrtAuthHeader implements AuthHeader {

  private final String value;
  private final Action action;

  public CrtAuthHeader(final String action, final String value) {
    switch (action) {
      case "request":
        this.action = Action.REQUEST;
        break;
      case "response":
        this.action = Action.RESPONSE;
        break;
      default:
        throw new IllegalArgumentException("CrtAuthHeader got invalid auth action " + action);
    }
    this.value = value;
  }

  @Override
  public Action getAction() {
    return action;
  }

  @Override
  public String getValue() {
    return value;
  }
}
