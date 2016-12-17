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

import java.util.List;
import org.apache.curator.framework.api.transaction.CuratorTransaction;

public class DeleteMany implements ZooKeeperOperation {

  private final List<String> paths;

  public DeleteMany(final List<String> paths) {
    this.paths = paths;
  }

  @Override
  public void register(final CuratorTransaction transaction) throws Exception {
    for (final String path : paths) {
      transaction.delete().forPath(path);
    }
  }

  @Override
  public String toString() {
    return "DeleteMany{"
           + "paths=" + paths
           + '}';
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final DeleteMany that = (DeleteMany) obj;

    return paths != null ? paths.equals(that.paths) : that.paths == null;

  }

  @Override
  public int hashCode() {
    return paths != null ? paths.hashCode() : 0;
  }
}
