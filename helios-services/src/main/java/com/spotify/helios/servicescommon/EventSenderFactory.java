/*
 * Copyright (c) 2016 Spotify AB.
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

import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class EventSenderFactory implements Supplier<List<EventSender>> {

  private static final Logger log = LoggerFactory.getLogger(EventSenderFactory.class);

  private final CommonConfiguration<?> config;

  public EventSenderFactory(final CommonConfiguration<?> config) {
    this.config = config;
  }

  @Override
  public List<EventSender> get() {
    final List<EventSender> senders = new ArrayList<>();

    final KafkaClientProvider kafkaClientProvider =
        new KafkaClientProvider(config.getKafkaBrokers());

    final Optional<KafkaProducer<String, byte[]>> kafkaProducer =
        kafkaClientProvider.getDefaultProducer();

    if (kafkaProducer.isPresent()) {
      senders.add(new KafkaSender(kafkaProducer));
    }

    final PubSub pubsub = PubSubOptions.getDefaultInstance().getService();

    for (final String prefix : config.getPubsubPrefixes()) {
      senders.add(GooglePubSubSender.create(pubsub, prefix));
    }

    return senders;
  }
}
