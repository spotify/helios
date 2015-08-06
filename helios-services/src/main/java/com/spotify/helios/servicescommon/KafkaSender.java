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

package com.spotify.helios.servicescommon;

import com.google.common.base.Optional;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A class that wraps {@link org.apache.kafka.clients.producer.KafkaProducer}.
 */
public class KafkaSender {

  private static final Logger log = LoggerFactory.getLogger(KafkaSender.class);

  private static final int KAFKA_SEND_TIMEOUT = 5;

  private final Optional<KafkaProducer<String, byte[]>> kafkaProducer;

  public KafkaSender(final Optional<KafkaProducer<String, byte[]>> kafkaProducer) {
    this.kafkaProducer = kafkaProducer;
  }

  public void send(final KafkaRecord record) {
    if (kafkaProducer.isPresent()) {
      try {
        final Future<RecordMetadata> future = kafkaProducer.get().send(
            new ProducerRecord<String, byte[]>(record.getKafkaTopic(), record.getKafkaData()));
        final RecordMetadata metadata = future.get(KAFKA_SEND_TIMEOUT, TimeUnit.SECONDS);
        log.debug("Sent an event to Kafka, meta: {}", metadata);
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        log.warn("Unable to send an event to Kafka", e);
      }
    } else {
      log.debug("KafkaProducer isn't set. Not sending anything.");
    }
  }
}
