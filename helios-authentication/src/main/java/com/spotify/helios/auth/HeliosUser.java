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

package com.spotify.helios.auth;

import com.google.common.base.Preconditions;

/** Represents an authenticated user accessing resources within the Helios master service. */
public class HeliosUser {

  private final String username;

  public HeliosUser(String username) {
    this.username = Preconditions.checkNotNull(username);
  }

  public String getUsername() {
    return username;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HeliosUser that = (HeliosUser) o;

    return username.equals(that.username);

  }

  @Override
  public int hashCode() {
    return username.hashCode();
  }
}
