package com.spotify.helios.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class HostPickingStrategiesTest {
  private static final int NUM_ITERATIONS = 1000;
  private static final ImmutableList<String> HOSTS =
      ImmutableList.of("hosta", "hostb", "hostc", "hostd");

  @Test
  public void testDeterministicOneHost() {
    final Set<String> chosenHosts = Sets.newHashSet();
    final HostPickingStrategy strategy1 = HostPickingStrategies.deterministicOneHost("");
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      chosenHosts.add(strategy1.pickHost(HOSTS));
    }

    final HostPickingStrategy strategy2 = HostPickingStrategies.deterministicOneHost("");
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      chosenHosts.add(strategy2.pickHost(HOSTS));
    }
    assertEquals(1, chosenHosts.size());
  }

  @Test
  public void testDeterministic() {
    final List<String> order = Lists.newArrayList();
    final Set<String> chosenHosts = Sets.newHashSet();
    final HostPickingStrategy strategy1 = HostPickingStrategies.deterministic("");
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final String picked = strategy1.pickHost(HOSTS);
      order.add(picked);
      chosenHosts.add(picked);
    }
    // should've hit them all
    assertEquals(HOSTS.size(), chosenHosts.size());

    final HostPickingStrategy strategy2 = HostPickingStrategies.deterministic("");
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      assertEquals("at index " + i, order.get(i), strategy2.pickHost(HOSTS));
    }
  }

  @Test
  public void testRandomOneHost() {
    final Set<String> chosenHosts = Sets.newHashSet();
    final HostPickingStrategy strategy1 = HostPickingStrategies.randomOneHost();
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      chosenHosts.add(strategy1.pickHost(HOSTS));
    }
    assertEquals(1, chosenHosts.size());
  }

  @Test
  public void testRandom() {
    final List<String> order = Lists.newArrayList();
    final Set<String> chosenHosts = Sets.newHashSet();
    final HostPickingStrategy strategy1 = HostPickingStrategies.random();
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final String picked = strategy1.pickHost(HOSTS);
      order.add(picked);
      chosenHosts.add(picked);
    }
    // should've hit them all
    assertEquals(HOSTS.size(), chosenHosts.size());

    final HostPickingStrategy strategy2 = HostPickingStrategies.random();
    boolean different = false;
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      if (!order.get(i).equals(strategy2.pickHost(HOSTS))) {
        different = true;
        break;
      }
    }
    assertTrue(different);
  }
}
