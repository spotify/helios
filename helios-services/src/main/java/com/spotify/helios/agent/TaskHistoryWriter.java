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

import com.google.common.annotations.VisibleForTesting;

import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.servicescommon.QueueingHistoryWriter;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Writes task history to ZK.
 */
public class TaskHistoryWriter extends QueueingHistoryWriter<TaskStatusEvent> {

  private final String hostname;

  @Override
  protected String getKey(final TaskStatusEvent event) {
    return event.getStatus().getJob().getId().toString();
  }

  @Override
  protected long getTimestamp(final TaskStatusEvent event) {
    return event.getTimestamp();
  }

  @Override
  protected byte[] toBytes(final TaskStatusEvent event) {
    return event.getStatus().toJsonBytes();
  }

  @Override
  protected String getZkEventsPath(TaskStatusEvent event) {
    final JobId jobId = event.getStatus().getJob().getId();
    return Paths.historyJobHostEvents(jobId, hostname);
  }

  public TaskHistoryWriter(final String hostname, final ZooKeeperClient client,
                           final Path backingFile) throws IOException, InterruptedException {
    super(client, backingFile);
    this.hostname = hostname;
  }

  public void saveHistoryItem(final TaskStatus status)
      throws InterruptedException {
    saveHistoryItem(status, System.currentTimeMillis());
  }

  public void saveHistoryItem(final TaskStatus status, long timestamp)
      throws InterruptedException {
    add(new TaskStatusEvent(status, timestamp, hostname));
  }

  @Override @VisibleForTesting
  protected void startUp() throws Exception {
    super.startUp();
  }
}
