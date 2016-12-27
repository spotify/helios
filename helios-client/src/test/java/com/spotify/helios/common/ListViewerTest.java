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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ListViewerTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final List<Integer> LIST_OF_TEN = ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  private static final List<Integer> EMPTY_LIST = Collections.emptyList();

  @Test
  public void testLowerBound() throws Exception {
    final List<Integer> localSublist = ListViewer.localSublist(0, 3, LIST_OF_TEN);
    assertThat(localSublist, contains(0, 1, 2, 3));
  }

  @Test
  public void testUpperBound() throws Exception {
    final List<Integer> localSublist = ListViewer.localSublist(10, 3, LIST_OF_TEN);
    assertThat(localSublist, contains(6, 7, 8, 9));
  }

  @Test
  public void testListSmallerThanContext() throws Exception {
    final List<Integer> list = ImmutableList.of(0, 1, 2);
    final List<Integer> localSublist = ListViewer.localSublist(1, 3, list);
    assertThat(localSublist, contains(0, 1, 2));
  }

  @Test
  public void testListLargerThanContext() throws Exception {
    final List<Integer> list = ImmutableList.of(0, 1, 2, 3, 4);
    final List<Integer> localSublist = ListViewer.localSublist(1, 1, list);
    assertThat(localSublist, contains(0, 1, 2));
  }

  @Test
  public void testContextIs0() throws Exception {
    final List<Integer> localSublist = ListViewer.localSublist(4, 0, LIST_OF_TEN);
    assertThat(localSublist, contains(4));
  }

  @Test
  public void testContextLessThan0() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("context has to be a non-negative integer.");
    ListViewer.localSublist(1, -1, LIST_OF_TEN);
  }

  @Test
  public void testEmptyList() throws Exception {
    List<Integer> localSublist = ListViewer.localSublist(1, 3, EMPTY_LIST);
    assertThat(localSublist, equalTo(EMPTY_LIST));

    localSublist = ListViewer.localSublist(1, 0, EMPTY_LIST);
    assertThat(localSublist, equalTo(EMPTY_LIST));
  }
}
