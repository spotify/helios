/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.servicescommon;

import com.google.common.base.Objects;
import com.google.common.base.Supplier;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.common.Json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.base.Charsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class PersistentAtomicReference<T> {

  private static final Logger log = LoggerFactory.getLogger(PersistentAtomicReference.class);

  private final Path filename;
  private final Path tempfilename;
  private final Object sync = new Object() {};

  private volatile T value;

  private PersistentAtomicReference(final Path filename,
                                    final TypeReference<T> typeReference,
                                    final Supplier<? extends T> initialValue)
      throws IOException {
    this.filename = filename.toAbsolutePath();
    this.tempfilename = filename.getFileSystem().getPath(this.filename.toString() + ".tmp");
    if (Files.exists(filename)) {
      final byte[] bytes = Files.readAllBytes(filename);
      value = Json.read(bytes, typeReference);
    } else {
      value = initialValue.get();
    }
  }

  public void set(T newValue) throws IOException {
    log.debug("set: ({}) {}", filename, newValue);
    synchronized (sync) {
      final String json = Json.asPrettyStringUnchecked(newValue);
      Files.write(tempfilename, json.getBytes(UTF_8));
      Files.move(tempfilename, filename, ATOMIC_MOVE, REPLACE_EXISTING);
      this.value = newValue;
    }
  }

  public T get() {
    return value;
  }

  public static <T> PersistentAtomicReference<T> create(final Path filename,
                                                        final TypeReference<T> typeReference,
                                                        final Supplier<? extends T> initialValue)
      throws IOException {
    return new PersistentAtomicReference<>(filename, typeReference, initialValue);
  }


  public static <T> PersistentAtomicReference<T> create(final String filename,
                                                        final TypeReference<T> typeReference,
                                                        final Supplier<? extends T> initialValue)
      throws IOException {
    return create(FileSystems.getDefault().getPath(filename), typeReference, initialValue);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("filename", filename)
        .toString();
  }
}
