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

package com.spotify.helios.agent;

import com.google.common.collect.ImmutableList;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class AgentParserTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testValidateArgument() {
    final Predicate<Integer> isOdd = num -> num % 2 == 1;

    final UnaryOperator<List<Integer>> testFn =
        nums -> AgentParser.validateArgument(nums, isOdd, n -> n + " is expected to be odd");

    testFn.apply(ImmutableList.of(1, 3, 5, 7));

    exception.expect(IllegalArgumentException.class);
    testFn.apply(ImmutableList.of(1, 3, 5, 7, 8));
  }
}
