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

package com.spotify.helios.servicescommon.statistics;

import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.state.ConnectionState;

/**
 * This interface lets us report ZooKeeper metrics.
 */
public interface ZooKeeperMetrics {

  /**
   * Call this to report a transient ZooKeeper error.
   */
  void zookeeperTransientError();

  /**
   * Call this to update the appropriate timer with an event duration.
   *
   * @param name     Durations with the same name get recorded under the same timer.
   * @param duration Duration of the event.
   * @param timeUnit Time unit of the duration.
   */
  void updateTimer(String name, long duration, TimeUnit timeUnit);

  void connectionStateChanged(ConnectionState newState);
}
