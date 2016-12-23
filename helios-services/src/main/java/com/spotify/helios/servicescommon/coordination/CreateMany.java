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

import java.util.Map;
import org.apache.curator.framework.api.transaction.CuratorTransaction;

public class CreateMany implements ZooKeeperOperation {

  private final Map<String, byte[]> nodes;

  public CreateMany(final Map<String, byte[]> nodes) {
    this.nodes = nodes;
  }

  @Override
  public void register(final CuratorTransaction transaction) throws Exception {
    for (final Map.Entry<String, byte[]> entry : nodes.entrySet()) {
      transaction.create().forPath(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public String toString() {
    return "CreateMany{"
           + "nodes=" + nodes.keySet()
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

    final CreateMany that = (CreateMany) obj;

    return nodes != null ? nodes.equals(that.nodes) : that.nodes == null;

  }

  @Override
  public int hashCode() {
    return nodes != null ? nodes.hashCode() : 0;
  }
}
