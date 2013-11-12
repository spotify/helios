/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.Map;

import static com.fasterxml.jackson.databind.MapperFeature.SORT_PROPERTIES_ALPHABETICALLY;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;

public class Json {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .configure(SORT_PROPERTIES_ALPHABETICALLY, true)
      .configure(ORDER_MAP_ENTRIES_BY_KEYS, true);

  private static final TypeReference<Map<String, Object>> MAP_TYPE =
      new TypeReference<Map<String, Object>>() {};

  public static byte[] asBytes(final Object value) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsBytes(value);
  }

  public static String asString(final Object value) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(value);
  }

  public static <T> T read(final byte[] bytes, final Class<T> clazz) throws IOException {
    return OBJECT_MAPPER.readValue(bytes, clazz);
  }

  public static <T> T read(final byte[] bytes, final TypeReference<?> typeReference)
      throws IOException {
    return OBJECT_MAPPER.readValue(bytes, typeReference);
  }

  public static <T> T read(final byte[] bytes, final JavaType javaType)
      throws IOException {
    return OBJECT_MAPPER.readValue(bytes, javaType);
  }

  public static MappingIterator<Map<String, Object>> readValues(
      final InputStream stream, final TypeReference<Map<String, Object>> typeReference)
      throws IOException {
    final JsonParser parser = OBJECT_MAPPER.getFactory().createParser(stream);
    return OBJECT_MAPPER.readValues(parser, typeReference);
  }

  public static JavaType type(Type t) {
    return OBJECT_MAPPER.constructType(t);
  }

  public static JavaType type(final TypeReference<?> typeReference) {
    return OBJECT_MAPPER.getTypeFactory().constructType(typeReference);
  }

  public static byte[] sha1digest(final Object o) throws IOException {
    final String json = OBJECT_MAPPER.writeValueAsString(o);
    final Map<String, Object> map = OBJECT_MAPPER.readValue(json, MAP_TYPE);
    return sha1digest(map);
  }

  public static byte[] sha1digest(final Map<String, ?> o) throws IOException {
    final byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(o);
    return Hash.sha1digest(bytes);
  }
}
