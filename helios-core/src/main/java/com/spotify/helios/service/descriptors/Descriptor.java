/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.service.descriptors;

import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.spotify.helios.common.Json;

import java.io.IOException;

public abstract class Descriptor {

  public String toJsonString() {
    try {
      return Json.asString(this);
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  public byte[] toJsonBytes() {
    try {
      return Json.asBytes(this);
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  public ByteString toJsonByteString() {
    return ByteString.copyFrom(toJsonBytes());
  }

  public static <T extends Descriptor> T parse(final byte[] bytes, Class<T> clazz)
      throws IOException {
    return Json.read(bytes, clazz);
  }

  public static <T extends Descriptor> T parse(final ByteString bytes, Class<T> clazz)
      throws IOException {
    return parse(bytes.toByteArray(), clazz);
  }
}
