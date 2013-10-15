/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.service.descriptors;

import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public abstract class Descriptor {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  public String toJsonString() {
    try {
      return objectMapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  public byte[] toJsonBytes() {
    try {
      return objectMapper.writeValueAsBytes(this);
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  public ByteString toJsonByteString() {
    return ByteString.copyFrom(toJsonBytes());
  }

  public static <T extends Descriptor> T parse(final byte[] bytes, Class<T> clazz)
      throws IOException {
    return objectMapper.readValue(bytes, clazz);
  }

  public static <T extends Descriptor> T parse(final ByteString bytes, Class<T> clazz)
      throws IOException {
    return parse(bytes.toByteArray(), clazz);
  }
}
