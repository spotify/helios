/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.cli;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Utils {

  public static <K, V> Map<K, V> allAsMap(final Map<K, ListenableFuture<V>> map)
      throws ExecutionException, InterruptedException {
    final Map<K, V> result = Maps.newHashMap();
    for (Map.Entry<K, ListenableFuture<V>> e : map.entrySet()) {
      result.put(e.getKey(), e.getValue().get());
    }
    return result;
  }
}
