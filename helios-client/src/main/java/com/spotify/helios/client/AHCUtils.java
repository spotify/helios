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

package com.spotify.helios.client;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class AHCUtils {

  /** Transforms an array of Headers to a Map. */
  static Map<String, List<String>> toMap(final Header[] headers) {
    final Map<String, List<String>> map = new HashMap<>(headers.length);
    for (Header header : headers) {
      // TODO (mbrown): in Java 8 this becomes Map.computeIfAbsent(key, function<K, V>)
      if (!map.containsKey(header.getName())) {
        map.put(header.getName(), new ArrayList<String>());
      }
      map.get(header.getName()).add(header.getValue());
    }
    return map;
  }

  static Header[] toArray(Map<String, List<String>> headers) {
    // oh I miss Java 8 and streams
    List<Header> list = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      final String name = entry.getKey();
      final List<String> values = entry.getValue();
      for (String value : values) {
        list.add(new BasicHeader(name, value));
      }
    }
    return list.toArray(new Header[list.size()]);
  }
}
