/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.spotify.helios.common.descriptors.PortMapping;

import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PortAllocatorTest {

  @Test
  public void testAllocate() throws Exception {
    final PortAllocator sut = new PortAllocator(10, 20);
    final Map<String, PortMapping> mapping = ImmutableMap.of("p1", PortMapping.of(17),
                                                             "p2", PortMapping.of(18, 18));
    final Set<Integer> used = ImmutableSet.of(10, 11);
    final Map<String, Integer> allocation = sut.allocate(mapping, used);
    assertEquals(ImmutableMap.of("p1", 12, "p2", 18), allocation);
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
    final PortAllocator sut = new PortAllocator(10, 11);
    final Map<String, PortMapping> mapping = ImmutableMap.of("p1", PortMapping.of(1),
                                                             "p2", PortMapping.of(18, 18));
    final Set<Integer> used = ImmutableSet.of();
    final Map<String, Integer> allocation = sut.allocate(mapping, used);
    assertEquals(ImmutableMap.of("p1", 10, "p2", 18), allocation);
  }
}
