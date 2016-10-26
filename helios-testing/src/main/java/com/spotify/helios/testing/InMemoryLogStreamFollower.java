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

import com.google.common.io.Closer;
import com.spotify.docker.client.LogStream;
import com.spotify.helios.common.descriptors.JobId;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link LogStreamFollower} implementation that captures all container stdout/stderr to memory.
 * Provided mostly for debugging and other limited use cases, since extended container output
 * will be kept in memory without bound.
 */
public class InMemoryLogStreamFollower implements LogStreamFollower {

  private final ConcurrentMap<JobId, StreamHolder> streamHolders;

  private InMemoryLogStreamFollower(
      final ConcurrentMap<JobId, StreamHolder> streamHolders) {
    this.streamHolders = streamHolders;
  }

  public static InMemoryLogStreamFollower create() {
    return new InMemoryLogStreamFollower(new ConcurrentHashMap<JobId, StreamHolder>());
  }

  /**
   * Get all the stdout that has been emitted by a container.
   *
   * @param jobId The {@link JobId} for the container in question.
   * @return A byte array of everything written to stdout.
   */
  public byte[] getStdout(final JobId jobId) {
    return streamHolders.get(jobId).stdout.toByteArray();
  }

  /**
   * Get all the stderr that has been emitted by a container.
   *
   * @param jobId The {@link JobId} for the container in question.
   * @return A byte array of everything written to stderr.
   */
  public byte[] getStderr(final JobId jobId) {
    return streamHolders.get(jobId).stderr.toByteArray();
  }

  @Override
  public void followLog(final JobId jobId, final String containerId, final LogStream logStream)
      throws IOException {
    final Closer closer = Closer.create();
    try {
      final ByteArrayOutputStream stdout = closer.register(new ByteArrayOutputStream());
      final ByteArrayOutputStream stderr = closer.register(new ByteArrayOutputStream());
      streamHolders.put(jobId, new StreamHolder(stdout, stderr));

      logStream.attach(stdout, stderr);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  private static final class StreamHolder {

    final ByteArrayOutputStream stdout;
    final ByteArrayOutputStream stderr;

    StreamHolder(final ByteArrayOutputStream stdout, final ByteArrayOutputStream stderr) {
      this.stdout = stdout;
      this.stderr = stderr;
    }
  }
}
