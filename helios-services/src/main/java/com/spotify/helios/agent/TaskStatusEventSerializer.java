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

import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.TaskStatusEvent;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Provides serialization support for KafkaProducer used for History subsystem
 * in {@link com.spotify.helios.agent.QueueingHistoryWriter}.
 */

public class TaskStatusEventSerializer implements Serializer<TaskStatusEvent> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

    @Override
    public byte[] serialize(String topic, TaskStatusEvent value) {
        return Json.asBytesUnchecked(value);
    }

    @Override
    public void close() { }
}
