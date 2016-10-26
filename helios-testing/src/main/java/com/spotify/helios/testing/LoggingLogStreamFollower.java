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

import com.google.common.base.Charsets;
import com.google.common.io.Closer;
import com.spotify.docker.client.LogMessage;
import com.spotify.docker.client.LogStream;
import com.spotify.helios.common.descriptors.JobId;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.EnumMap;
import java.util.Map;
import org.slf4j.Logger;

final class LoggingLogStreamFollower implements LogStreamFollower {

  private final Logger log;

  private LoggingLogStreamFollower(final Logger log) {
    this.log = log;
  }

  public static LoggingLogStreamFollower create(final Logger log) {
    return new LoggingLogStreamFollower(log);
  }

  @Override
  public void followLog(final JobId jobId, final String containerId, final LogStream logStream)
      throws IOException {
    final Closer closer = Closer.create();
    final Map<LogMessage.Stream, CharsetDecoder> streamDecoders = createStreamDecoders();
    final StringBuilder buffer = new StringBuilder();

    LogMessage.Stream lastStream = null;

    try {
      while (logStream.hasNext()) {
        final LogMessage message = logStream.next();
        final ByteBuffer content = message.content();
        final LogMessage.Stream stream = message.stream();

        if (lastStream != null && lastStream != stream && buffer.length() > 0) {
          log(lastStream, containerId, jobId, buffer);
        }

        final CharBuffer charBuffer = streamDecoders.get(stream).decode(content);

        while (charBuffer.hasRemaining()) {
          final char c = charBuffer.get();

          switch (c) {
            case '\n':
              log(stream, containerId, jobId, buffer);
              break;
            default:
              buffer.append(c);
          }
        }

        lastStream = stream;
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      if (lastStream != null) {
        log(lastStream, containerId, jobId, buffer);
      }
      closer.close();
    }
  }

  private Map<LogMessage.Stream, CharsetDecoder> createStreamDecoders() {
    final Map<LogMessage.Stream, CharsetDecoder> streamDecoders =
        new EnumMap<>(LogMessage.Stream.class);

    for (final LogMessage.Stream stream : LogMessage.Stream.values()) {
      final CharsetDecoder decoder = Charsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPLACE)
          .onUnmappableCharacter(CodingErrorAction.REPLACE);
      streamDecoders.put(stream, decoder);
    }

    return streamDecoders;
  }

  private void log(
      final LogMessage.Stream stream,
      final String containerId,
      final JobId jobId,
      final StringBuilder buffer) {
    log.info("[{}] [{}] {} {}",
             jobId.getName(),
             containerId.substring(0, Math.min(7, containerId.length())),
             stream.id(),
             buffer.toString());
    buffer.setLength(0);
  }
}
