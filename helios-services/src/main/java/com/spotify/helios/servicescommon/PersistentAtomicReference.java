/*-
 * -\-\-
 * Helios Services
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

package com.spotify.helios.servicescommon;

import static com.google.common.base.Charsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.google.common.base.Supplier;
import com.spotify.helios.common.Json;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that is similar to {@code AtomicReference} but is backed by a file, so can be
 * persisted across a server restart.  Assumes the underlying type can be serialized by Jackson.
 *
 * <p>Strangely, this is not actually atomic in the {@code AtomicReference} way; i.e. not
 * threadsafe, nor does it do CAS.
 */
public class PersistentAtomicReference<T> {

  private static final Logger log = LoggerFactory.getLogger(PersistentAtomicReference.class);

  private final Path filename;
  private final Path tempfilename;
  private final Object sync = new Object();

  private volatile T value;

  private PersistentAtomicReference(final Path filename,
                                    final JavaType javaType,
                                    final Supplier<? extends T> initialValue)
      throws IOException, InterruptedException {
    try {
      this.filename = filename.toAbsolutePath();
      this.tempfilename = filename.getFileSystem().getPath(this.filename.toString() + ".tmp");
      if (Files.exists(filename)) {
        final byte[] bytes = Files.readAllBytes(filename);
        if (bytes.length > 0) {
          value = Json.read(bytes, javaType);
        } else {
          value = initialValue.get();
        }
      } else {
        value = initialValue.get();
      }
    } catch (InterruptedIOException | ClosedByInterruptException e) {
      throw new InterruptedException(e.getMessage());
    }
  }

  /**
   * Set the reference to {@code newValue}.
   *
   * @param newValue The value to set.
   *
   * @throws IOException          If an error occurs working with the file on disk.
   * @throws InterruptedException If the thread is interrupted.
   */
  public void set(T newValue) throws IOException, InterruptedException {
    try {
      set0(newValue);
    } catch (InterruptedIOException | ClosedByInterruptException e) {
      throw new InterruptedException(e.getMessage());
    }
  }

  private void set0(final T newValue) throws IOException {
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

  /**
   * Set the reference to {@code newValue}, and wraps {@link IOException}s in
   * {@link RuntimeException}s.
   *
   * @param newValue The value to set.
   *
   * @throws InterruptedException If the thread is interrupted.
   */
  public void setUnchecked(T newValue) throws InterruptedException {
    try {
      set(newValue);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the value stored.
   *
   * @return The value.
   */
  public T get() {
    return value;
  }

  public static <T> PersistentAtomicReference<T> create(final Path filename,
                                                        final TypeReference<T> typeReference,
                                                        final Supplier<? extends T> initialValue)
      throws IOException, InterruptedException {

    return new PersistentAtomicReference<>(filename, Json.type(typeReference), initialValue);
  }


  public static <T> PersistentAtomicReference<T> create(final String filename,
                                                        final TypeReference<T> typeReference,
                                                        final Supplier<? extends T> initialValue)
      throws IOException, InterruptedException {
    return create(FileSystems.getDefault().getPath(filename), typeReference, initialValue);
  }

  public static <T> PersistentAtomicReference<T> create(final Path filename,
                                                        final JavaType javaType,
                                                        final Supplier<? extends T> initialValue)
      throws IOException, InterruptedException {
    return new PersistentAtomicReference<>(filename, javaType, initialValue);
  }


  public static <T> PersistentAtomicReference<T> create(final String filename,
                                                        final JavaType javaType,
                                                        final Supplier<? extends T> initialValue)
      throws IOException, InterruptedException {
    return create(FileSystems.getDefault().getPath(filename), javaType, initialValue);
  }

  @Override
  public String toString() {
    return "PersistentAtomicReference{"
           + "filename=" + filename
           + '}';
  }
}
