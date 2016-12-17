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

package com.spotify.helios.servicescommon.coordination;

import org.apache.curator.framework.api.transaction.CuratorTransaction;

import java.util.Arrays;

public class SetData implements ZooKeeperOperation {

  private final byte[] bytes;
  private final String path;

  public SetData(String path, byte[] bytes) {
    this.path = path;
    this.bytes = bytes;
  }

  @Override
  public void register(CuratorTransaction transaction) throws Exception {
    transaction.setData().forPath(path, bytes);
  }

  @Override
  public String toString() {
    return "SetData{" +
           "path='" + path + '\'' +
           '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final SetData setData = (SetData) o;

    if (!Arrays.equals(bytes, setData.bytes)) {
      return false;
    }
    if (path != null ? !path.equals(setData.path) : setData.path != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = bytes != null ? Arrays.hashCode(bytes) : 0;
    result = 31 * result + (path != null ? path.hashCode() : 0);
    return result;
  }
}
