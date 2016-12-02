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
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class EventSenderFactory {

  private EventSenderFactory() {
  }

  public static List<EventSender> build(Environment environment, CommonConfiguration<?> config) {

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

    // register the senders with the lifecycle so they will be started/stopped when the
    // service starts and stops.
    final LifecycleEnvironment lifecycle = environment.lifecycle();
    lifecycle.manage(new ManagedPubSub(pubsub));
    senders.forEach(lifecycle::manage);

    return senders;
  }

  /** Small wrapper so we can close the PubSub instance when service shuts down. */
  private static final class ManagedPubSub implements Managed {

    private final PubSub pubsub;

    private ManagedPubSub(final PubSub pubsub) {
      this.pubsub = pubsub;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
      pubsub.close();
    }
  }
}
