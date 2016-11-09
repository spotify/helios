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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class GooglePubSubProvider {
  private static final Logger log = LoggerFactory.getLogger(GooglePubSubProvider.class);

  private final List<String> pubsubPrefixes;

  public GooglePubSubProvider(final List<String> pubsubPrefixes) {
    this.pubsubPrefixes = pubsubPrefixes;
  }

  public List<GooglePubSubSender> senders() {
    return senders(() -> PubSubOptions.getDefaultInstance().getService());
  }

  public List<GooglePubSubSender> senders(Supplier<PubSub> pubsubSupplier) {
    if (pubsubPrefixes != null && !pubsubPrefixes.isEmpty()) {
      try {
        final PubSub pubsub = pubsubSupplier.get();
        return pubsubPrefixes.stream()
            .map(prefix -> new GooglePubSubSender(pubsub, prefix))
            .collect(toList());
      } catch (Exception e) {
        log.warn("Failed to set up google pubsub service", e);
        return emptyList();
      }
    } else {
      return emptyList();
    }
  }
}
