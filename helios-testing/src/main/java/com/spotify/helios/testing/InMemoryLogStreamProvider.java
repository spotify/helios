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

package com.spotify.helios.testing;

import com.spotify.helios.common.descriptors.JobId;

import com.google.common.collect.Maps;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * A {@link LogStreamProvider} implementation that captures all container stdout/stderr to memory.
 * Provided mostly for debugging and other limited use cases, since extended container output
 * will be kept in memory without bound.
 */
public class InMemoryLogStreamProvider implements LogStreamProvider {

  private final Map<JobId, ByteArrayOutputStream> stdouts = Maps.newHashMap();
  private final Map<JobId, ByteArrayOutputStream> stderrs = Maps.newHashMap();

  @Override
  public OutputStream getStdoutStream(final JobId jobId, final String containerId) {
    return getStream(stdouts, jobId);
  }

  @Override
  public OutputStream getStderrStream(final JobId jobId, final String containerId) {
    return getStream(stderrs, jobId);
  }

  /**
   * Get all the stdout that has been emitted by a container.
   *
   * @param jobId The {@link JobId} for the container in question.
   * @return A byte array of everything written to stdout.
   */
  public byte[] getStdout(final JobId jobId) {
    return getBytes(stdouts, jobId);
  }

  /**
   * Get all the stderr that has been emitted by a container.
   *
   * @param jobId The {@link JobId} for the container in question.
   * @return A byte array of everything written to stderr.
   */
  public byte[] getStderr(final JobId jobId) {
    return getBytes(stderrs, jobId);
  }

  private static byte[] getBytes(final Map<JobId, ByteArrayOutputStream> streamMap,
                                 final JobId jobId) {
    if (streamMap.containsKey(jobId)) {
      return streamMap.get(jobId).toByteArray();
    } else {
      return new byte[0];
    }
  }

  private static OutputStream getStream(final Map<JobId, ByteArrayOutputStream> streamMap,
                                        final JobId jobId) {
    if (!streamMap.containsKey(jobId)) {
      synchronized (streamMap) {
        if (!streamMap.containsKey(jobId)) {
          streamMap.put(jobId, new ByteArrayOutputStream());
        }
      }
    }

    return streamMap.get(jobId);
  }
}
