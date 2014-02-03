/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.spotify.helios.common.Json;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class Descriptor {

  public String toJsonString() {
    return Json.asStringUnchecked(this);
  }

  public byte[] toJsonBytes() {
    try {
      return Json.asBytes(this);
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T extends Descriptor> T parse(final byte[] bytes, Class<T> clazz)
      throws IOException {
    return Json.read(bytes, clazz);
  }

  public static <T extends Descriptor> T parse(final String value, Class<T> clazz)
      throws IOException {
    return parse(value.getBytes(UTF_8), clazz);
  }
}
