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
