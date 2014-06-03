package com.spotify.helios.agent.docker;

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Charsets.UTF_8;

public class LogStream extends AbstractIterator<LogMessage> implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(LogStream.class);

  private final LogReader reader;
  private volatile boolean closed;

  LogStream(final InputStream stream) {
    this.reader = new LogReader(stream);
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      log.warn(this + " not closed properly");
      close();
    }
  }

  @Override
  protected LogMessage computeNext() {
    final LogMessage message;
    try {
      message = reader.nextMessage();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    if (message == null) {
      return endOfData();
    }
    return message;
  }

  @Override
  public void close() {
    closed = true;
    try {
      reader.close();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public String readFully() {
    StringBuilder stringBuilder = new StringBuilder();
    while (hasNext()) {
      stringBuilder.append(UTF_8.decode(next().content()));
    }
    return stringBuilder.toString();
  }
}
