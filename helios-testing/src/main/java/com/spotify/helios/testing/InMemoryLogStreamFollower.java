/*-
 * -\-\-
 * Helios Testing Library
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

package com.spotify.helios.testing;

import com.spotify.docker.client.LogMessage;
import com.spotify.helios.common.descriptors.JobId;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
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
   *
   * @return A byte array of everything written to stdout.
   */
  public byte[] getStdout(final JobId jobId) {
    return streamHolders.get(jobId).stdout.toByteArray();
  }

  /**
   * Get all the stderr that has been emitted by a container.
   *
   * @param jobId The {@link JobId} for the container in question.
   *
   * @return A byte array of everything written to stderr.
   */
  public byte[] getStderr(final JobId jobId) {
    return streamHolders.get(jobId).stderr.toByteArray();
  }

  @Override
  public void followLog(
      final JobId jobId, final String containerId, final Iterator<LogMessage> logStream)
      throws IOException {
    try (final ByteArrayOutputStream stdout = new ByteArrayOutputStream();
         final ByteArrayOutputStream stderr = new ByteArrayOutputStream()) {
      streamHolders.put(jobId, new StreamHolder(stdout, stderr));

      while (logStream.hasNext()) {
        final LogMessage message = logStream.next();
        final ByteBuffer content = message.content();

        switch (message.stream()) {
          case STDOUT:
            writeAndFlush(content, stdout);
            break;
          case STDERR:
            writeAndFlush(content, stderr);
            break;
          case STDIN:
          default:
            break;
        }
      }
    }
  }

  /** Write the contents of the given ByteBuffer to the OutputStream and flush the stream. */
  private static void writeAndFlush(
      final ByteBuffer buffer, final OutputStream outputStream) throws IOException {

    if (buffer.hasArray()) {
      outputStream.write(buffer.array(), buffer.position(), buffer.remaining());
    } else {
      // cannot access underlying byte array, need to copy into a temporary array
      while (buffer.hasRemaining()) {
        // figure out how much to read, but use an upper limit of 8kb. LogMessages should be rather
        // small so we don't expect this to get hit but avoid large temporary buffers, just in case.
        final int size = Math.min(buffer.remaining(), 8 * 1024);
        final byte[] chunk = new byte[size];
        buffer.get(chunk);
        outputStream.write(chunk);
      }
    }
    outputStream.flush();
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
