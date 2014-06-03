package com.spotify.helios.agent.docker;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkNotNull;

public class LogMessage {

  final Stream stream;
  final ByteBuffer content;

  public LogMessage(final int streamId, final ByteBuffer content) {
    this(Stream.of(streamId), content);
  }

  public LogMessage(final Stream stream, final ByteBuffer content) {
    this.stream = checkNotNull(stream, "stream");
    this.content = checkNotNull(content, "content");
  }

  public Stream stream() {
    return stream;
  }

  public ByteBuffer content() {
    return content.asReadOnlyBuffer();
  }

  public enum Stream {
    STDIN(0),
    STDOUT(1),
    STDERR(2);

    private final int id;

    Stream(int id) {
      this.id = id;
    }

    public int id() {
      return id;
    }

    public static Stream of(final int id) {
      switch (id) {
        case 0:
          return STDIN;
        case 1:
          return STDOUT;
        case 2:
          return STDERR;
        default:
          throw new IllegalArgumentException();
      }
    }
  }
}
