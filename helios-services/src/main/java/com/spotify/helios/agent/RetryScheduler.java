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

public interface RetryScheduler {

  /**
   * Get the current interval. Updated when {@link #nextMillis()} is called.
   *
   * @return The current interval in milliseconds.
   */
  long currentMillis();

  /**
   * Progress to and return a new interval. {@link #currentMillis()} will return this interval until
   * the next invocation of {@link #nextMillis()}.
   *
   * @return The new interval in milliseconds.
   */
  long nextMillis();
}
