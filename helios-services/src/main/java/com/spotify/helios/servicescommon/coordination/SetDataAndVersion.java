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

package com.spotify.helios.servicescommon.coordination;

import com.google.common.base.Objects;
import org.apache.curator.framework.api.transaction.CuratorTransaction;

class SetDataAndVersion implements ZooKeeperOperation {

  private final byte[] bytes;
  private final String path;
  private int version;

  SetDataAndVersion(String path, byte[] bytes, final int version) {
    this.path = path;
    this.bytes = bytes;
    this.version = version;
  }

  @Override
  public void register(CuratorTransaction transaction) throws Exception {
    transaction.setData().withVersion(version).forPath(path, bytes);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SetDataAndVersion that = (SetDataAndVersion) o;
    return version == that.version &&
           Objects.equal(bytes, that.bytes) &&
           Objects.equal(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(bytes, path, version);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("bytes", bytes)
        .add("path", path)
        .add("version", version)
        .toString();
  }
}
