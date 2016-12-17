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

public class VersionedValue<T> {

  private final T value;
  private final int version;

  public static <T> VersionedValue<T> of(final T value, int version) {
    return new VersionedValue<>(value, version);
  }

  public VersionedValue(final T value, final int version) {
    this.value = value;
    this.version = version;
  }

  public T value() {
    return value;
  }

  public int version() {
    return version;
  }
}
