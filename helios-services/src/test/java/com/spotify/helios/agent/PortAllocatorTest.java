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

package com.spotify.helios.agent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.spotify.helios.common.descriptors.PortMapping;

import org.junit.Test;

import java.net.ServerSocket;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PortAllocatorTest {

  @Test
  public void testAllocate() throws Exception {
    final PortAllocator sut = new PortAllocator(20000, 20010);
    final Map<String, PortMapping> mapping = ImmutableMap.of("p1", PortMapping.of(17),
                                                             "p2", PortMapping.of(18, 18));
    final Set<Integer> used = ImmutableSet.of(10, 11);
    final Map<String, Integer> allocation = sut.allocate(mapping, used);
    assertEquals(ImmutableMap.of("p1", 20000, "p2", 18), allocation);
  }

  @Test
  public void testInsufficientPortsFail1() throws Exception {
    final PortAllocator sut = new PortAllocator(10, 11);
    final Map<String, PortMapping> mapping = ImmutableMap.of("p1", PortMapping.of(17),
                                                             "p2", PortMapping.of(18, 18));
    final Set<Integer> used = ImmutableSet.of(10, 11);
    final Map<String, Integer> allocation = sut.allocate(mapping, used);
    assertNull(allocation);
  }

  @Test
  public void testInsufficientPortsFail2() throws Exception {
    final PortAllocator sut = new PortAllocator(10, 11);
    final Map<String, PortMapping> mapping = ImmutableMap.of("p1", PortMapping.of(1),
                                                             "p2", PortMapping.of(2),
                                                             "p3", PortMapping.of(4),
                                                             "p4", PortMapping.of(18, 18));
    final Set<Integer> used = ImmutableSet.of();
    final Map<String, Integer> allocation = sut.allocate(mapping, used);
    assertNull(allocation);
  }

  @Test
  public void verifyStaticPortsNotCheckedAgainstRange() throws Exception {
    final PortAllocator sut = new PortAllocator(20000, 20001);
    final Map<String, PortMapping> mapping = ImmutableMap.of("p1", PortMapping.of(1),
                                                             "p2", PortMapping.of(18, 18));
    final Set<Integer> used = ImmutableSet.of();
    final Map<String, Integer> allocation = sut.allocate(mapping, used);
    assertEquals(ImmutableMap.of("p1", 20000, "p2", 18), allocation);
  }

  @Test
  public void testAllocateCheckSystem() throws Exception {
    final PortAllocator sut = new PortAllocator(20000, 20010);
    final Map<String, PortMapping> mapping = ImmutableMap.of("p1", PortMapping.of(17),
                                                             "p2", PortMapping.of(18, 20001));
    final Set<Integer> used = Collections.emptySet();

    // Occupy a port and check that Helios doesn't use it.
    new ServerSocket(20000);
    final Map<String, Integer> allocation = sut.allocate(mapping, used);
    assertEquals(ImmutableMap.of("p1", 20002, "p2", 20001), allocation);
  }
}
