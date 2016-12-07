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
  private final boolean performHealthchecks;

  public EventSenderFactory(final CommonConfiguration<?> config,
                            final boolean performHealthchecks) {
    this.config = config;
    this.performHealthchecks = performHealthchecks;
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

    final GooglePubSubProvider googlePubSubProvider =
        new GooglePubSubProvider(config.getPubsubPrefixes());

    senders.addAll(googlePubSubProvider.senders());

    if (performHealthchecks) {
      // filter out any senders that fail the healthcheck
      senders.removeIf(sender -> !sender.isHealthy());
    }

    log.info("health eventSenders: {}", senders);

    return senders;
  }
}
