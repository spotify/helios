/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
