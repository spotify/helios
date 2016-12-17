/*-
 * -\-\-
 * Helios Tools
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

package com.spotify.helios.cli;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Common test utilities.
 */
public class TestUtils {

  /**
   * Get the first column from an output of a table whose entries are separated by newlines and
   * columns are separated by spaces.
   */
  public static List<String> readFirstColumnFromOutput(final String output,
                                                       final boolean skipFirstRow) {
    final List<String> values = Lists.newArrayList();
    for (final String line : output.split("\n")) {
      final String col = line.split(" ")[0];
      values.add(col);
    }
    if (skipFirstRow) {
      return values.subList(1, values.size());
    }
    return values;
  }
}
