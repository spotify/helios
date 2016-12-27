/*-
 * -\-\-
 * Helios Client
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

package com.spotify.helios.common;

import static java.util.Collections.singletonList;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Utility methods for viewing large Lists.
 */
public class ListViewer {

  /**
   * Returns an immutable local view of the list based on the index and context.
   * Useful if you have a large list and want to print a local view of it.
   */
  public static <T> List<T> localSublist(final int index,
                                         final int context,
                                         final List<T> list) {
    Preconditions.checkNotNull(list);
    if (list.isEmpty()) {
      return list;
    } else if (context < 0) {
      throw new IllegalArgumentException("context has to be a non-negative integer.");
    } else if (context == 0) {
      return singletonList(list.get(index));
    } else if (list.size() < (context * 2 + 1)) {
      return list;
    }

    final int lowerIndex = Math.max(0, index - context - 1);
    final int upperIndex = Math.min(list.size(), index + context + 1);
    return ImmutableList.copyOf(list.subList(lowerIndex, upperIndex));
  }

}
