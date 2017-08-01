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

import static java.lang.Integer.toHexString;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to manage job prefix files on disk, which TemporaryJobs uses to keep track
 * of old jobs which need to be cleaned up. The class allows you to create/delete files, and
 * will handle all management of file locks and channels. When an instance of this class is no
 * longer needed either {@link #release}, {@link #close} or {@link #delete} should be called so
 * that the underlying FileChannel can be properly closed.
 */
class JobPrefixFile implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(JobPrefixFile.class);

  private final String prefix;
  private final Path file;

  private FileChannel channel;
  private FileLock lock;

  /**
   * Creates a JobPrefixFile using a randomly generated filename in the specified directory.
   *
   * @return a new JobPrefixFile
   */
  public static JobPrefixFile create(Path directory) throws IOException {
    return create(null, directory);
  }

  /**
   * Creates a JobPrefixFile using the specified prefix and directory.
   *
   * @param prefix    the job prefix, which will be the name of the file
   * @param directory the directory where the file will be created
   *
   * @return a new JobPrefixFile
   */
  public static JobPrefixFile create(final String prefix, final Path directory)
      throws IOException {
    return new JobPrefixFile(prefix, directory);
  }

  /**
   * Attempts to lock the given file, and create a JobPrefixFile for it. A new JobPrefixFile
   * instance will be returned if a lock can be obtained for the file. Null will be returned if the
   * lock is already held by either this process or another. For all other cases, an exception will
   * be thrown.
   *
   * @param file the path to the file
   *
   * @return a new JobPrefixFile if a file lock can be obtained. Null if a lock for the file is
   *         already held by either this process or another.
   */
  public static JobPrefixFile tryFromExistingFile(final Path file) throws IOException {
    Preconditions.checkNotNull(file);
    final FileChannel channel = FileChannel.open(file, WRITE);
    final FileLock lock;

    try {
      // We want to return JobPrefixFile if we can obtain the lock, null if someone already holds
      // the lock, and throw an exception in all other cases. tryLock makes this a little tricky.
      // It's behavior this:
      //   - returns a FileLock if one can be obtained
      //   - returns null if another process holds the lock
      //   - throws OverlappingFileLockException if the lock is already held by this process
      //   - throws a various exceptions for other errors
      lock = channel.tryLock();
    } catch (OverlappingFileLockException e) {
      // If the lock is already held by this process, close the channel and return null.
      close(channel);
      return null;
    } catch (Exception e) {
      // If an unexpected error occurred, close the channel and rethrow.
      close(channel);
      throw e;
    }

    // If another process hold the lock, close the channel and return null.
    if (lock == null) {
      close(channel);
      return null;
    }

    // If we've obtained the lock, return a new JobPrefixFile
    return new JobPrefixFile(file, channel, lock);
  }

  private JobPrefixFile(final String prefix, final Path directory) throws IOException {
    Preconditions.checkNotNull(directory);
    this.prefix = prefix == null
                  ? "tmp-" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "-"
                    + toHexString(ThreadLocalRandom.current().nextInt())
                  : prefix;

    // Make sure directory exists, then create prefix file
    Files.createDirectories(directory);
    file = directory.resolve(this.prefix);
    final Path tmp = directory.resolve(this.prefix + ".tmp");
    try {
      // If we didn't create the file with the .tmp extension, there would be a race condition
      // where another process could read the file and try to delete it before we obtained the lock
      this.channel = FileChannel.open(tmp, CREATE_NEW, WRITE);
      this.lock = channel.lock();
      Files.move(tmp, file);
    } catch (Exception e) {
      deleteIfExists(tmp);
      deleteIfExists(file);
      close(this.channel);
      throw new RuntimeException("Failed to create job prefix file " + file, e);
    }
  }

  private JobPrefixFile(final Path file, final FileChannel channel, final FileLock lock)
      throws IOException, IllegalStateException {
    this.file = Preconditions.checkNotNull(file, "file");
    this.channel = Preconditions.checkNotNull(channel, "channel");
    this.lock = Preconditions.checkNotNull(lock, "lock");
    this.prefix = file.getFileName().toString();
  }

  /**
   * Deletes the file, cleaning up any associated resources. This will not throw an exception.
   * Either this method, {@link #release} or {@link #close} should be called when the instance is
   * no longer needed.
   */
  public void delete() {
    release();
    deleteIfExists(file);
  }

  /**
   * Helper method to delete file if it exists. This will not thrown an exception.
   *
   * @param file the file to delete
   */
  private void deleteIfExists(Path file) {
    if (file != null) {
      try {
        // Use deleteIfExists because there is a slight chance another process deleted the file
        // just after we deleted the lock.
        Files.deleteIfExists(file);
      } catch (Exception e) {
        log.warn("Failed to delete file {}", file, e);
      }
    }
  }

  /**
   * Return the job prefix which is the same as the file name.
   *
   * @return the job prefix
   */
  public String prefix() {
    return prefix;
  }

  /**
   * Release the lock. If the lock has already been released, the method returns immediately. This
   * method will not throw an exception. Either this method, {@link #close} or {@link #delete}
   * should be called when the instance is no longer needed.
   */
  public void release() {
    close(lock);
    lock = null;
    close(channel);
    channel = null;
  }

  /**
   * Invokes {@link #release()}. Added so JobPrefixFile can be used within a try-with-resources
   * statement. Either this method, {@link #release} or {@link #delete} should be called when the
   * instance is no longer needed.
   */
  @Override
  public void close() {
    release();
  }

  /**
   * Helper method for closing any object which implements {@link java.lang.AutoCloseable}.
   * Exceptions are swallowed, and nulls ignored.
   *
   * @param closeable the object to close
   */
  private static void close(final AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Exception e) {
        log.debug("Failed to close {}", closeable.getClass().getSimpleName(), e);
      }
    }
  }

}
