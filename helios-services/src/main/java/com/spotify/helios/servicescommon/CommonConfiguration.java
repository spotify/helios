/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.servicescommon;

import io.dropwizard.Configuration;

import java.util.List;

public class CommonConfiguration<C extends CommonConfiguration<C>> extends Configuration {

  private List<String> kafkaBrokers;
  private List<String> pubsubPrefixes;

  public List<String> getKafkaBrokers() {
    return kafkaBrokers;
  }

  @SuppressWarnings("unchecked")
  public C setKafkaBrokers(List<String> kafkaBrokers) {
    this.kafkaBrokers = kafkaBrokers;
    return (C) this;
  }

  public List<String> getPubsubPrefixes() {
    return pubsubPrefixes;
  }

  @SuppressWarnings("unchecked")
  public C setPubsubPrefixes(List<String> pubsubPrefixes) {
    this.pubsubPrefixes = pubsubPrefixes;
    return (C) this;
  }
}
