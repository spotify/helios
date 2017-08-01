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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.spotify.docker.client.LogMessage;
import com.spotify.helios.common.descriptors.JobId;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("AvoidEscapedUnicodeCharacters")
public class LoggingLogStreamFollowerTest {

  private final LoggerContext context = new LoggerContext();
  private final Logger log = context.getLogger("test");
  private final CapturingAppender appender = CapturingAppender.create();

  @Before
  public void setUp() throws Exception {
    context.start();
    appender.start();
    log.setLevel(Level.ALL);
    log.addAppender(appender);
  }

  @After
  public void tearDown() throws Exception {
    appender.stop();
    context.stop();
  }

  @Test
  public void testSingleLine() throws Exception {
    final Iterator<LogMessage> stream = stream(stdout("abc123\n"));

    final LoggingLogStreamFollower sut = LoggingLogStreamFollower.create(log);
    sut.followLog(JobId.fromString("a:b:c"), "d", stream);

    assertThat(appender.events(),
        contains(event(Level.INFO, "[a] [d] 1 abc123")));
  }

  @Test
  public void testSingleLineNoNewline() throws Exception {
    final Iterator<LogMessage> stream = stream(stdout("abc123"));

    final LoggingLogStreamFollower sut = LoggingLogStreamFollower.create(log);
    sut.followLog(JobId.fromString("a:b:c"), "d", stream);

    assertThat(appender.events(),
        contains(event(Level.INFO, "[a] [d] 1 abc123")));
  }

  @Test
  public void testTwoLines() throws Exception {
    final Iterator<LogMessage> stream = stream(stdout("abc123\n123abc\n"));

    final LoggingLogStreamFollower sut = LoggingLogStreamFollower.create(log);
    sut.followLog(JobId.fromString("a:b:c"), "d", stream);

    assertThat(appender.events(),
        contains(event(Level.INFO, "[a] [d] 1 abc123"),
            event(Level.INFO, "[a] [d] 1 123abc")));
  }

  @Test
  public void testTwoLinesNoNewline() throws Exception {
    final Iterator<LogMessage> stream = stream(stdout("abc123\n123abc"));

    final LoggingLogStreamFollower sut = LoggingLogStreamFollower.create(log);
    sut.followLog(JobId.fromString("a:b:c"), "d", stream);

    assertThat(appender.events(),
        contains(event(Level.INFO, "[a] [d] 1 abc123"),
            event(Level.INFO, "[a] [d] 1 123abc")));
  }

  @Test
  public void testInterleavedStreams() throws Exception {
    final Iterator<LogMessage> stream =
        stream(stdout("abc"), stderr("123"), stdout("123"), stderr("abc"));

    final LoggingLogStreamFollower sut = LoggingLogStreamFollower.create(log);
    sut.followLog(JobId.fromString("a:b:c"), "d", stream);

    assertThat(appender.events(),
        contains(event(Level.INFO, "[a] [d] 1 abc"),
            event(Level.INFO, "[a] [d] 2 123"),
            event(Level.INFO, "[a] [d] 1 123"),
            event(Level.INFO, "[a] [d] 2 abc")));
  }

  @Test
  public void testInterleavedStreamsNewline() throws Exception {
    final Iterator<LogMessage> stream =
        stream(stdout("abc\n"), stderr("123\n"), stdout("123\n"), stderr("abc\n"));

    final LoggingLogStreamFollower sut = LoggingLogStreamFollower.create(log);
    sut.followLog(JobId.fromString("a:b:c"), "d", stream);

    assertThat(appender.events(),
        contains(event(Level.INFO, "[a] [d] 1 abc"),
            event(Level.INFO, "[a] [d] 2 123"),
            event(Level.INFO, "[a] [d] 1 123"),
            event(Level.INFO, "[a] [d] 2 abc")));
  }

  @Test
  public void testPartialUtf8() throws Exception {
    final byte[] annoyingData = "foo\u0000\uffff௵\uD808\uDC30".getBytes(Charsets.UTF_8);
    assertThat(annoyingData.length, is(14));

    final Iterator<LogMessage> stream =
        stream(stdout(Arrays.copyOfRange(annoyingData, 0, 1)),
            stdout(Arrays.copyOfRange(annoyingData, 1, 2)),
            stdout(Arrays.copyOfRange(annoyingData, 2, 3)),
            stdout(Arrays.copyOfRange(annoyingData, 3, 4)),
            stdout(Arrays.copyOfRange(annoyingData, 4, 5)),
            stdout(Arrays.copyOfRange(annoyingData, 5, 6)),
            stdout(Arrays.copyOfRange(annoyingData, 6, 7)),
            stdout(Arrays.copyOfRange(annoyingData, 7, 8)),
            stdout(Arrays.copyOfRange(annoyingData, 8, 9)),
            stdout(Arrays.copyOfRange(annoyingData, 9, 10)),
            stdout(Arrays.copyOfRange(annoyingData, 10, 11)),
            stdout(Arrays.copyOfRange(annoyingData, 11, 12)),
            stdout(Arrays.copyOfRange(annoyingData, 12, 13)),
            stdout(Arrays.copyOfRange(annoyingData, 13, 14)));

    final LoggingLogStreamFollower sut = LoggingLogStreamFollower.create(log);
    sut.followLog(JobId.fromString("a:b:c"), "d", stream);

    assertThat(appender.events(),
        contains(event(Level.INFO, "[a] [d] 1 foo\u0000\uffff௵\uD808\uDC30")));
  }

  @Test
  public void testPartialUtf8Interleaved() throws Exception {
    final byte[] annoyingData = "foo\u0000\uffff௵\uD808\uDC30".getBytes(Charsets.UTF_8);
    assertThat(annoyingData.length, is(14));

    final Iterator<LogMessage> stream =
        stream(stdout(Arrays.copyOfRange(annoyingData, 0, 1)),
            stderr(Arrays.copyOfRange(annoyingData, 0, 1)),
            stdout(Arrays.copyOfRange(annoyingData, 1, 2)),
            stderr(Arrays.copyOfRange(annoyingData, 1, 2)),
            stdout(Arrays.copyOfRange(annoyingData, 2, 3)),
            stderr(Arrays.copyOfRange(annoyingData, 2, 3)),
            stdout(Arrays.copyOfRange(annoyingData, 3, 4)),
            stderr(Arrays.copyOfRange(annoyingData, 3, 4)),
            stdout(Arrays.copyOfRange(annoyingData, 4, 5)),
            stderr(Arrays.copyOfRange(annoyingData, 4, 5)),
            stdout(Arrays.copyOfRange(annoyingData, 5, 6)),
            stderr(Arrays.copyOfRange(annoyingData, 5, 6)),
            stdout(Arrays.copyOfRange(annoyingData, 6, 7)),
            stderr(Arrays.copyOfRange(annoyingData, 6, 7)),
            stdout(Arrays.copyOfRange(annoyingData, 7, 8)),
            stderr(Arrays.copyOfRange(annoyingData, 7, 8)),
            stdout(Arrays.copyOfRange(annoyingData, 8, 9)),
            stderr(Arrays.copyOfRange(annoyingData, 8, 9)),
            stdout(Arrays.copyOfRange(annoyingData, 9, 10)),
            stderr(Arrays.copyOfRange(annoyingData, 9, 10)),
            stdout(Arrays.copyOfRange(annoyingData, 10, 11)),
            stderr(Arrays.copyOfRange(annoyingData, 10, 11)),
            stdout(Arrays.copyOfRange(annoyingData, 11, 12)),
            stderr(Arrays.copyOfRange(annoyingData, 11, 12)),
            stdout(Arrays.copyOfRange(annoyingData, 12, 13)),
            stderr(Arrays.copyOfRange(annoyingData, 12, 13)),
            stdout(Arrays.copyOfRange(annoyingData, 13, 14)),
            stderr(Arrays.copyOfRange(annoyingData, 13, 14)));

    final LoggingLogStreamFollower sut = LoggingLogStreamFollower.create(log);
    sut.followLog(JobId.fromString("a:b:c"), "d", stream);

    assertThat(appender.events(),
        contains(event(Level.INFO, "[a] [d] 1 f"),
            event(Level.INFO, "[a] [d] 2 f"),
            event(Level.INFO, "[a] [d] 1 o"),
            event(Level.INFO, "[a] [d] 2 o"),
            event(Level.INFO, "[a] [d] 1 o"),
            event(Level.INFO, "[a] [d] 2 o"),
            event(Level.INFO, "[a] [d] 1 \u0000"),
            event(Level.INFO, "[a] [d] 2 \u0000"),
            event(Level.INFO, "[a] [d] 1 \uffff"),
            event(Level.INFO, "[a] [d] 2 \uffff"),
            event(Level.INFO, "[a] [d] 1 ௵"),
            event(Level.INFO, "[a] [d] 2 ௵"),
            event(Level.INFO, "[a] [d] 1 \uD808\uDC30"),
            event(Level.INFO, "[a] [d] 2 \uD808\uDC30")));
  }

  @Test
  public void testDropPartialUtf8Sequence() throws Exception {
    final byte[] annoyingData = "௵".getBytes(Charsets.UTF_8);
    assertThat(annoyingData.length, is(3));

    final Iterator<LogMessage> stream =
        stream(stdout(Arrays.copyOfRange(annoyingData, 0, 1)));

    final LoggingLogStreamFollower sut = LoggingLogStreamFollower.create(log);
    sut.followLog(JobId.fromString("a:b:c"), "d", stream);

    assertThat(appender.events(), is(emptyCollectionOf(ILoggingEvent.class)));
  }

  @Test
  public void testTruncateContainerId() throws Exception {
    final Iterator<LogMessage> stream = stream(stdout("x"));

    final LoggingLogStreamFollower sut = LoggingLogStreamFollower.create(log);
    sut.followLog(JobId.fromString("a:b:c"), "0123456789abcdef", stream);

    assertThat(appender.events(), contains(event(Level.INFO, "[a] [0123456] 1 x")));
  }

  private static Matcher<ILoggingEvent> event(Level level, String formattedMessage) {
    return allOf(
        hasProperty("level", is(level)),
        hasProperty("formattedMessage", is(formattedMessage))
    );
  }

  private static Iterator<LogMessage> stream(LogMessage... messages) {
    return ImmutableList.copyOf(messages).iterator();
  }

  private static LogMessage stdout(String chunk) {
    return message(LogMessage.Stream.STDOUT, chunk);
  }

  private static LogMessage stdout(byte[] chunk) {
    return message(LogMessage.Stream.STDOUT, chunk);
  }

  private static LogMessage stderr(byte[] chunk) {
    return message(LogMessage.Stream.STDERR, chunk);
  }

  private static LogMessage stderr(String chunk) {
    return message(LogMessage.Stream.STDERR, chunk);
  }

  private static LogMessage message(
      final LogMessage.Stream stream, final String chunk) {
    return message(stream, chunk.getBytes(Charsets.UTF_8));
  }

  private static LogMessage message(
      final LogMessage.Stream stream, final byte[] chunk) {
    final ByteBuffer buffer = ByteBuffer.wrap(chunk);
    return new LogMessage(stream, buffer.asReadOnlyBuffer());
  }
}
