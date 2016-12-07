/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * A class that wraps {@link org.apache.kafka.clients.producer.KafkaProducer}.
 */
public class KafkaSender implements EventSender {

  private static final Logger log = LoggerFactory.getLogger(KafkaSender.class);

  private final Optional<KafkaProducer<String, byte[]>> kafkaProducer;

  public KafkaSender(final Optional<KafkaProducer<String, byte[]>> kafkaProducer) {
    this.kafkaProducer = kafkaProducer;
  }

  @Override
  public boolean isHealthy() {
    return true;
  }

  private void send(final KafkaRecord kafkaRecord) {
    if (kafkaProducer.isPresent()) {
      final ProducerRecord<String, byte[]> record =
          new ProducerRecord<>(kafkaRecord.getKafkaTopic(), kafkaRecord.getKafkaData());

      kafkaProducer.get().send(record, new LoggingCallback());
    } else {
      log.debug("KafkaProducer isn't set. Not sending anything.");
    }
  }

  @Override
  public void send(final String topic, final byte[] message) {
    send(KafkaRecord.of(topic, message));
  }

  private static class LoggingCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
      if (e == null) {
        log.debug("Sent an event to Kafka, meta: {}", metadata);
      } else {
        log.warn("Unable to send an event to Kafka", e);
      }
    }
  }
}
