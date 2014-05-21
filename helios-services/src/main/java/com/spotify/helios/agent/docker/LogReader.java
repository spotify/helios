package com.spotify.helios.agent.docker;

import com.google.common.io.ByteStreams;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class LogReader implements Closeable {

  private final InputStream stream;
  public static final int HEADER_SIZE = 8;
  public static final int FRAME_SIZE_OFFSET = 4;

  public LogReader(final InputStream stream) {
    this.stream = stream;
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  public LogMessage nextMessage() throws IOException {

    // Read header
    final byte[] headerBytes = new byte[HEADER_SIZE];
    final int n = ByteStreams.read(stream, headerBytes, 0, HEADER_SIZE);
    if (n == 0) {
      return null;
    }
    if (n != HEADER_SIZE) {
      throw new EOFException();
    }
    final ByteBuffer header = ByteBuffer.wrap(headerBytes);
    final int streamId = header.get();
    header.position(FRAME_SIZE_OFFSET);
    final int frameSize = header.getInt();

    // Read frame
    final byte[] frame = new byte[frameSize];
    ByteStreams.readFully(stream, frame);
    return new LogMessage(streamId, ByteBuffer.wrap(frame));
  }
}