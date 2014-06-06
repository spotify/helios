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

package com.spotify.helios.common;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.Map;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.MapperFeature.SORT_PROPERTIES_ALPHABETICALLY;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;

public class Json {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .configure(SORT_PROPERTIES_ALPHABETICALLY, true)
      .configure(ORDER_MAP_ENTRIES_BY_KEYS, true)
      .configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static final ObjectWriter NORMALIZING_OBJECT_WRITER = new ObjectMapper()
      .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
      .configure(SORT_PROPERTIES_ALPHABETICALLY, true)
      .configure(ORDER_MAP_ENTRIES_BY_KEYS, true)
      .writer();

  private static final ObjectWriter PRETTY_OBJECT_WRITER = new ObjectMapper()
      .configure(SORT_PROPERTIES_ALPHABETICALLY, true)
      .configure(ORDER_MAP_ENTRIES_BY_KEYS, true)
      .writerWithDefaultPrettyPrinter();

  private static final TypeReference<Map<String, Object>> MAP_TYPE =
      new TypeReference<Map<String, Object>>() {};

  /**
   * Serialize an object to json. Use when it is not know whether an object can be json
   * serializable.
   *
   * @see #asBytesUnchecked(Object)
   */
  public static byte[] asBytes(final Object value) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsBytes(value);
  }

  /**
   * Serialize an object to json. Use when object is expected to be json serializable.
   *
   * @see #asBytes(Object)
   */
  public static byte[] asBytesUnchecked(final Object value) {
    try {
      return OBJECT_MAPPER.writeValueAsBytes(value);
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Serialize an object to a json string. Use when it is not know whether an object can be json
   * serializable.
   *
   * @see #asStringUnchecked(Object)
   */
  public static String asString(final Object value) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(value);
  }

  /**
   * Serialize an object to a json string. Use when object is expected to be json serializable.
   *
   * @see #asString(Object)
   */
  public static String asStringUnchecked(final Object value) {
    try {
      return asString(value);
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Serialize an object to a json string. Use when it is not know whether an object can be json
   * serializable.
   *
   * @see #asPrettyStringUnchecked(Object)
   */
  public static String asPrettyString(final Object value) throws JsonProcessingException {
    return PRETTY_OBJECT_WRITER.writeValueAsString(value);
  }

  /**
   * Serialize an object to a json string. Use when object is expected to be json serializable.
   *
   * @see #asPrettyString(Object)
   */
  public static String asPrettyStringUnchecked(final Object value) {
    try {
      return asPrettyString(value);
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Serialize an object to a json string, ordering fields and omitting null and empty fields.
   * Use when it is not know whether an object can be json serializable.
   *
   * @see #asPrettyStringUnchecked(Object)
   */
  public static String asNormalizedString(final Object value) throws JsonProcessingException {
    return NORMALIZING_OBJECT_WRITER.writeValueAsString(value);
  }

  /**
   * Serialize an object to a json string, ordering fields and omitting null and empty fields.
   * Use when object is expected to be json serializable.
   *
   * @see #asPrettyString(Object)
   */
  public static String asNormalizedStringUnchecked(final Object value) {
    try {
      return asNormalizedString(value);
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> T read(final String content, final Class<T> clazz) throws IOException {
    return OBJECT_MAPPER.readValue(content, clazz);
  }

  public static <T> T read(final String content, final TypeReference<?> typeReference)
      throws IOException {
    return OBJECT_MAPPER.readValue(content, typeReference);
  }

  public static <T> T read(final String content, final JavaType javaType)
      throws IOException {
    return OBJECT_MAPPER.readValue(content, javaType);
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

  public static <T> T readUnchecked(final String content, final Class<T> clazz) {
    try {
      return OBJECT_MAPPER.readValue(content, clazz);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> T readUnchecked(final String content, final TypeReference<?> typeReference) {
    try {
      return OBJECT_MAPPER.readValue(content, typeReference);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> T readUnchecked(final String content, final JavaType javaType) {
    try {
      return OBJECT_MAPPER.readValue(content, javaType);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> T readUnchecked(final byte[] bytes, final Class<T> clazz) {
    try {
      return OBJECT_MAPPER.readValue(bytes, clazz);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> T readUnchecked(final byte[] bytes, final TypeReference<?> typeReference) {
    try {
      return OBJECT_MAPPER.readValue(bytes, typeReference);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> T readUnchecked(final byte[] bytes, final JavaType javaType) {
    try {
      return OBJECT_MAPPER.readValue(bytes, javaType);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static MappingIterator<Map<String, Object>> readValues(
      final InputStream stream, final TypeReference<Map<String, Object>> typeReference)
      throws IOException {
    final JsonParser parser = OBJECT_MAPPER.getFactory().createParser(stream);
    return OBJECT_MAPPER.readValues(parser, typeReference);
  }

  public static MappingIterator<JsonNode> readValues(
      final InputStream stream)
      throws IOException {
    final JsonParser parser = OBJECT_MAPPER.getFactory().createParser(stream);
    return OBJECT_MAPPER.readValues(parser, JsonNode.class);
  }

  public static JsonNode readTree(final byte[] bytes) throws IOException {
    return OBJECT_MAPPER.readTree(bytes);
  }

  public static JsonNode readTreeUnchecked(final byte[] bytes){
    try {
      return readTree(bytes);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static JsonNode readTree(final String content) throws IOException {
    return OBJECT_MAPPER.readTree(content);
  }

  public static JsonNode readTreeUnchecked(final String content){
    try {
      return readTree(content);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static JsonNode readTree(final File file) throws IOException {
    return OBJECT_MAPPER.readTree(file);
  }

  public static JsonNode readTreeUnchecked(final File file){
    try {
      return readTree(file);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static JavaType type(Type t) {
    return OBJECT_MAPPER.constructType(t);
  }

  public static JavaType type(final TypeReference<?> typeReference) {
    return OBJECT_MAPPER.getTypeFactory().constructType(typeReference);
  }

  public static TypeFactory typeFactory() {
    return OBJECT_MAPPER.getTypeFactory();
  }

  public static ObjectReader reader() {
    return OBJECT_MAPPER.reader();
  }

  public static ObjectWriter writer() {
    return OBJECT_MAPPER.writer();
  }

  public static byte[] sha1digest(final Object o) throws IOException {
    final String json = NORMALIZING_OBJECT_WRITER.writeValueAsString(o);
    final Map<String, Object> map = OBJECT_MAPPER.readValue(json, MAP_TYPE);
    return sha1digest(map);
  }

  public static byte[] sha1digest(final Map<String, ?> o) throws IOException {
    final byte[] bytes = NORMALIZING_OBJECT_WRITER.writeValueAsBytes(o);
    return Hash.sha1digest(bytes);
  }
}
