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

import com.google.common.base.Charsets;
import com.spotify.docker.client.LogMessage;
import com.spotify.helios.common.descriptors.JobId;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;

/**
 * Follows a Docker log stream by logging it to a {@link Logger}.
 */
final class LoggingLogStreamFollower implements LogStreamFollower {

  private final Logger log;

  private LoggingLogStreamFollower(final Logger log) {
    this.log = log;
  }

  /**
   * Creates a new logging log stream follower.
   *
   * @param log the log to forward logs to
   *
   * @return a new instance
   */
  public static LoggingLogStreamFollower create(final Logger log) {
    return new LoggingLogStreamFollower(log);
  }

  @Override
  public void followLog(
      final JobId jobId, final String containerId, final Iterator<LogMessage> logStream)
      throws IOException {
    final Map<LogMessage.Stream, Decoder> streamDecoders = createStreamDecoders();
    final StringBuilder stringBuilder = new StringBuilder();

    LogMessage.Stream lastStream = null;

    try {
      while (logStream.hasNext()) {
        final LogMessage message = logStream.next();
        final ByteBuffer content = message.content();
        final LogMessage.Stream stream = message.stream();

        if (lastStream != null && lastStream != stream && stringBuilder.length() > 0) {
          log(lastStream, containerId, jobId, stringBuilder);
        }

        final Decoder decoder = streamDecoders.get(stream);
        final CharsetDecoder charsetDecoder = decoder.charsetDecoder;
        final ByteBuffer byteBuffer = decoder.byteBuffer;
        final CharBuffer charBuffer = decoder.charBuffer;

        while (content.hasRemaining()) {
          // Transfer as much of content into byteBuffer that we have room for
          byteBuffer.put(content);
          byteBuffer.flip();

          // Decode as much of byteBuffer into charBuffer that we can
          charsetDecoder.decode(byteBuffer, charBuffer, false);

          // The decoder might have left some partial byte sequences in the byteBuffer... Since we
          // don't have a ring buffer we should compact the buffer to not overflow.
          // We MUST NOT clear the byteBuffer since then we can lose those partial byte sequences.
          byteBuffer.compact();

          // Now start consuming the charBuffer
          charBuffer.flip();

          // Heuristic to avoid allocations... this will allocate too much memory if the charBuffer
          // contains any newlines or other special chars
          stringBuilder.ensureCapacity(charBuffer.remaining());

          while (charBuffer.hasRemaining()) {
            final char c = charBuffer.get();

            switch (c) {
              case '\n':
                log(stream, containerId, jobId, stringBuilder);
                break;
              default:
                stringBuilder.append(c);
            }
          }

          // This buffer is completely drained so we can reset it
          charBuffer.clear();
        }

        lastStream = stream;
      }
    } finally {
      if (lastStream != null && stringBuilder.length() > 0) {
        // Yes, we are not checking for any trailing bytes in the decoder byteBuffers here.  That
        // means that if the container wrote partial UTF-8 sequences before EOF they will be
        // discarded.
        log(lastStream, containerId, jobId, stringBuilder);
      }
    }
  }

  /**
   * Creates charset decoders for all available log message streams.
   *
   * @return a map containing a decoder for every log message stream type
   */
  private Map<LogMessage.Stream, Decoder> createStreamDecoders() {
    final Map<LogMessage.Stream, Decoder> streamDecoders =
        new EnumMap<>(LogMessage.Stream.class);

    for (final LogMessage.Stream stream : LogMessage.Stream.values()) {
      final CharsetDecoder charsetDecoder = Charsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPLACE)
          .onUnmappableCharacter(CodingErrorAction.REPLACE);
      final ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
      final CharBuffer charBuffer = CharBuffer.allocate(1024);
      streamDecoders.put(stream, new Decoder(charsetDecoder, byteBuffer, charBuffer));
    }

    return streamDecoders;
  }

  private void log(
      final LogMessage.Stream stream,
      final String containerId,
      final JobId jobId,
      final StringBuilder stringBuilder) {
    log.info("[{}] [{}] {} {}",
        jobId.getName(),
        containerId.substring(0, Math.min(7, containerId.length())),
        stream.id(),
        stringBuilder.toString());
    stringBuilder.setLength(0);
  }

  private static final class Decoder {

    // The decoder whose job is to transfer decoded bytes from byteBuffer to charBuffer
    final CharsetDecoder charsetDecoder;
    // As of yet unencoded bytes (might contain partial UTF-8 sequences)
    final ByteBuffer byteBuffer;
    // Characters that have not yet been written to a string builder
    final CharBuffer charBuffer;

    private Decoder(final CharsetDecoder charsetDecoder,
                    final ByteBuffer byteBuffer,
                    final CharBuffer charBuffer) {
      this.charsetDecoder = charsetDecoder;
      this.byteBuffer = byteBuffer;
      this.charBuffer = charBuffer;
    }
  }
}
