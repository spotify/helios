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

package com.spotify.helios.common.descriptors;

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;

@JsonSerialize(using = TypedInteger.Serializer.class)
public abstract class TypedInteger {
  protected static class Serializer extends JsonSerializer<TypedInteger> {
    @Override
    public void serialize(final TypedInteger value,
                          final JsonGenerator generator,
                          final SerializerProvider dummy)
        throws IOException, JsonProcessingException {
      if (value.get() == null) {
        generator.writeNull();
      } else {
        generator.writeNumber(value.get());
      }
    }
  }

  private final Integer value;

  protected TypedInteger(final Integer value) {
    Preconditions.checkNotNull(value);
    this.value = value;
  }

  public final Integer get() {
    return value;
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }

  @Override
  public int hashCode() {
    if (value == null) {
      return 0;
    }
    return value;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final TypedInteger other = (TypedInteger) obj;
    if (!value.equals(other.value)) {
      return false;
    }
    return true;
  }
}
