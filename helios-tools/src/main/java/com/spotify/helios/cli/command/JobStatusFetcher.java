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

package com.spotify.helios.cli.command;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

public class JobStatusFetcher {

  public static Map<JobId, ListenableFuture<JobStatus>> getJobsStatuses(HeliosClient client,
      Set<JobId> jobIds) throws InterruptedException {
    final Map<JobId, ListenableFuture<JobStatus>> futures = Maps.newTreeMap();
    try {
      final Map<JobId, JobStatus> statuses = client.jobStatuses(jobIds).get();
      for (final Entry<JobId, JobStatus> entry : statuses.entrySet()) {
        futures.put(entry.getKey(), Futures.immediateFuture(entry.getValue()));
      }
    } catch (final ExecutionException e) {
      System.err.println("Warning: masters failed batch status fetching.  Falling back to"
          + " slower job status method");
      for (final JobId jobId : jobIds) {
        futures.put(jobId, client.jobStatus(jobId));
      }
    }
    return futures;
  }

}
