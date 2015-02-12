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

package com.spotify.helios.agent;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.util.List;

public class KafkaClientProvider {
  private static final String KAFKA_HELIOS_CLIENT_ID = "Helios";
  private static final String KAFKA_QUORUM_PARAMETER = "1";

  private final Optional<ImmutableMap<String, Object>> partialConfigs;

  public KafkaClientProvider(@Nullable final List<String> brokerList) {
    partialConfigs = Optional.fromNullable(brokerList).transform(
        new Function<List<String>, ImmutableMap<String, Object>>() {
      @Nullable
      @Override
      public ImmutableMap<String, Object> apply(List<String> input) {
        return ImmutableMap.<String, Object>of(
            "bootstrap.servers", Joiner.on(',').join(input),
            "acks", KAFKA_QUORUM_PARAMETER,
            "client.id", KAFKA_HELIOS_CLIENT_ID);
        }
      });
    }

  public <K, V> Optional<KafkaProducer<K, V>> getProducer(@NotNull final Serializer<K> ks,
                                                          @NotNull final Serializer<V> vs) {
    return partialConfigs.transform(
        new Function<ImmutableMap<String, Object>, KafkaProducer<K, V>>() {
      @Nullable
      @Override
      public KafkaProducer<K, V> apply(ImmutableMap<String, Object> input) {
        return new KafkaProducer<>(input, ks, vs);
      }
    });
  }
}
