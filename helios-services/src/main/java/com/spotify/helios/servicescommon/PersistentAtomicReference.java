/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package com.spotify.helios.servicescommon;

import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

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
  private final Object sync = new Object();

  private volatile T value;

  private PersistentAtomicReference(final Path filename,
                                    final TypeReference<T> typeReference,
                                    final Supplier<? extends T> initialValue)
      throws IOException {
    this.filename = filename.toAbsolutePath();
    this.tempfilename = filename.getFileSystem().getPath(this.filename.toString() + ".tmp");
    if (Files.exists(filename)) {
      final byte[] bytes = Files.readAllBytes(filename);
      if (bytes.length > 0) {
        value = Json.read(bytes, typeReference);
      } else {
        value = initialValue.get();
      }
    } else {
      value = initialValue.get();
    }
  }

  public void set(T newValue) throws IOException {
    log.debug("set: ({}) {}", filename, newValue);
    synchronized (sync) {
      final String json = Json.asPrettyStringUnchecked(newValue);
      log.debug("write: ({}) {}", tempfilename, json);
      Files.write(tempfilename, json.getBytes(UTF_8));
      log.debug("move: {} -> {}", tempfilename, filename);
      Files.move(tempfilename, filename, ATOMIC_MOVE, REPLACE_EXISTING);
      this.value = newValue;
    }
  }

  public void setUnchecked(T newValue) {
    try {
      set(newValue);
    } catch (IOException e) {
      throw Throwables.propagate(e);
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
