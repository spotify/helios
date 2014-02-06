/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import java.io.PrintStream;
import java.util.List;

import static java.lang.Math.max;

/**
 * Produces tabulated output, adding padding to cells as necessary to align them into columns.
 */

public class Table {

  private final PrintStream out;
  private final String paddingString;
  private int[] columns = new int[0];
  private final List<Object[]> rows = Lists.newArrayList();


  public Table(final PrintStream out, final int padding) {
    this.out = out;
    this.paddingString = Strings.repeat(" ", padding);
  }

  public Table(final PrintStream out) {
    this(out, 4);
  }

  public void row(final Object... row) {
    columns = Ints.ensureCapacity(columns, row.length, row.length);
    for (int i = 0; i < row.length; i++) {
      row[i] = row[i].toString();
      columns[i] = max(columns[i], row[i].toString().length());
    }
    rows.add(row);
  }

  public void print() {
    for (final Object[] row : rows) {
      for (int i = 0; i < row.length; i++) {
        final String cell = row[i].toString();
        out.print(cell);
        out.print(paddingString);
        final int padding = columns[i] - cell.length();
        for (int j = 0; j < padding; j++) {
          out.print(' ');
        }
      }
      out.println();
    }
  }
}
