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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;

/**
 * Represents which of the IP protocols to use for a port mapping.
 *
 * TODO: USE ME!
 */
@JsonSerialize(using = IpProtocol.Serializer.class)
@JsonDeserialize(using = IpProtocol.Deserializer.class)
public enum IpProtocol {
  TCP("tcp"), UDP("udp");

  protected static class Serializer extends JsonSerializer<IpProtocol> {
    @Override
    public void serialize(final IpProtocol proto,
                          final JsonGenerator generator,
                          final SerializerProvider dummy)
        throws IOException, JsonProcessingException {
      generator.writeString(proto.get());
    }
  }

  protected static class Deserializer extends JsonDeserializer<IpProtocol> {
    @Override
    public IpProtocol deserialize(final JsonParser parser,
                                  final DeserializationContext arg1)
        throws IOException, JsonProcessingException {
      return of(parser.getValueAsString());
    }
  }

  private final String value;

  IpProtocol(final String value) {
    this.value = value;
  }

  public String get() {
    return value;
  }

  public static IpProtocol of(final String v) {
    if ("tcp".equals(v)) {
      return TCP;
    } else if ("udp".equals(v)) {
      return UDP;
    }
    throw new IllegalStateException("IpProtocol should be either tcp or udp, not " + v);
  }
}
