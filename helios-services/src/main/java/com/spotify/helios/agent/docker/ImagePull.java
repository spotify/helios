package com.spotify.helios.agent.docker;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.spotify.helios.common.Json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

class ImagePull implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(ImagePull.class);
  private final InputStream stream;

  private volatile boolean closed;

  ImagePull(final InputStream stream) {
    this.stream = stream;
  }

  void tail(final String image)
      throws DockerException {
    try {
      final MappingIterator<JsonNode> iterator = Json.readValues(stream);
      while (iterator.hasNextValue()) {
        final JsonNode message;
        message = iterator.nextValue();
        final JsonNode error = message.get("error");
        if (error != null) {
          if (error.toString().contains("404")) {
            throw new ImageNotFoundException(image, message.toString());
          } else {
            throw new ImagePullFailedException(image, message.toString());
          }
        }
        log.info("pull {}: {}", image, message);
      }
    } catch (IOException e) {
      throw new DockerException(e);
    }
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
  public void close() {
    closed = true;
    try {
      stream.close();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
