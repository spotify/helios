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

package com.spotify.helios;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TemporaryPorts extends ExternalResource {

  private static final Logger log = LoggerFactory.getLogger(TemporaryPorts.class);

  // Allocate below the linux default ephemeral port range
  private static final int DEFAULT_START = 20000;
  private static final int DEFAULT_END = 32768;
  private static final int DEFAULT_RETRIES = 100;
  private static final Path DEFAULT_LOCK_DIRECTORY = Paths.get("/tmp/helios-test/ports/");
  private static final boolean DEFAULT_RELEASE = false;

  private final List<AllocatedPort> ports = Lists.newArrayList();

  private final int start;
  private final int end;
  private final boolean release;
  private final int retries;
  private final Path lockDirectory;

  private volatile boolean closed;

  private TemporaryPorts(final Builder builder) {
    this.start = builder.start;
    this.end = builder.end;
    this.release = builder.release;
    this.retries = builder.retries;
    this.lockDirectory = checkNotNull(builder.lockDirectory, "lockDirectory");
    try {
      Files.createDirectories(lockDirectory);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        releasePorts();
      }
    });
  }

  @Override
  protected synchronized void after() {
    closed = true;
    if (release) {
      releasePorts();
    }
  }

  private void releasePorts() {
    for (AllocatedPort port : ports) {
      port.release();
    }
    ports.clear();
  }

  public synchronized AllocatedPort tryAcquire(final String name, final int port) {
    final AllocatedPort allocatedPort = lock(port, name);
    if (allocatedPort == null) {
      return null;
    }
    ports.add(allocatedPort);
    return allocatedPort;
  }

  public synchronized int localPort(final String name) {
    Preconditions.checkState(!closed, "closed");
    for (int i = 0; i < retries; i++) {
      final int port = randomPort();
      final AllocatedPort allocatedPort = lock(port, name);
      if (allocatedPort == null) {
        continue;
      }
      if (available(port)) {
        log.debug("allocated port \"{}\": {}", name, port);
        ports.add(allocatedPort);
        return port;
      } else {
        allocatedPort.release();
      }
    }
    throw new AllocationFailedException();
  }

  public synchronized Range<Integer> localPortRange(final String name, final int n) {
    Preconditions.checkState(!closed, "closed");
    for (int i = 0; i < retries; i++) {
      final int base = randomPort();
      final List<AllocatedPort> rangePorts = Lists.newArrayList();
      boolean successful = true;
      for (int j = 0; j < n; j++) {
        final int port = base + j;
        final AllocatedPort allocatedPort = lock(port, name);
        if (allocatedPort == null) {
          successful = false;
          break;
        }
        rangePorts.add(allocatedPort);
        if (!available(port)) {
          successful = false;
          break;
        }
      }
      if (successful) {
        ports.addAll(rangePorts);
        return Range.closedOpen(base, base + n);
      } else {
        for (AllocatedPort port : rangePorts) {
          port.release();
        }
      }
    }
    throw new AllocationFailedException();
  }

  private int randomPort() {
    return ThreadLocalRandom.current().nextInt(start, end);
  }

  @SuppressWarnings("ThrowFromFinallyBlock")
  private boolean available(int port) {
    // First probe by attempting to bind
    final Socket s = new Socket();
    try {
      s.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
    } catch (IOException e) {
      return false;
    } finally {
      try {
        s.close();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    // Now wait 15 seconds for kernel to consider port to be unused
    for (int i = 0; i < 15; i++) {
      final Process p;
      try {
        p = Runtime.getRuntime().exec("lsof -i:" + port);
        p.waitFor();
      } catch (IOException | InterruptedException e) {
        throw propagate(e);
      }
      if (p.exitValue() == 1) {
        return true;
      }
      log.debug("waiting for port {} to become unused", port);
      Uninterruptibles.sleepUninterruptibly(1, SECONDS);
    }

    // We lost a race with someone else taking the port into use
    return false;
  }

  private AllocatedPort lock(final int port, final String name) {
    final Path path = lockDirectory.resolve(String.valueOf(port));
    try {
      FileChannel file = FileChannel.open(path, CREATE, WRITE);
      FileLock lock = file.tryLock();
      if (lock == null) {
        return null;
      }
      file.write(ByteBuffer.wrap(format("%d %s%n", port, name).getBytes(UTF_8)));
      file.force(true);
      return new AllocatedPort(port, path, file, lock);
    } catch (OverlappingFileLockException e) {
      return null;
    } catch (IOException e) {
      log.error("Failed to take port lock: {}", path, e);
      throw Throwables.propagate(e);
    }
  }

  public static TemporaryPorts create() {
    return builder().build();
  }

  public static class AllocatedPort {

    private final int port;
    private final Path path;
    private final FileChannel file;
    private final FileLock lock;

    private AllocatedPort(final int port, final Path path, FileChannel file, FileLock lock) {
      this.port = port;
      this.path = path;
      this.file = file;
      this.lock = lock;
    }

    public int port() {
      return port;
    }

    public void release() {
      if (!lock.isValid()) {
        log.debug("lock already released: {}", path);
        return;
      }
      try {
        lock.release();
      } catch (Exception e) {
        log.warn("caught exception releasing port lock: {}", path, e);
      }
      try {
        file.close();
      } catch (Exception e) {
        log.warn("caught exception closing port lock file: {}", path, e);
      }
      try {
        Files.deleteIfExists(path);
      } catch (Exception e) {
        log.warn("caught exception deleting port lock file: {}", path, e);
      }
    }
  }

  public class AllocationFailedException extends RuntimeException {
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private int start = DEFAULT_START;
    private int end = DEFAULT_END;
    private Path lockDirectory = DEFAULT_LOCK_DIRECTORY;
    private boolean release = DEFAULT_RELEASE;
    private int retries = DEFAULT_RETRIES;

    public int start() {
      return start;
    }

    public Builder start(final int start) {
      this.start = start;
      return this;
    }

    public int end() {
      return end;
    }

    public Builder end(final int end) {
      this.end = end;
      return this;
    }

    public Path lockDirectory() {
      return lockDirectory;
    }

    public void lockDirectory(final Path lockDirectory) {
      this.lockDirectory = lockDirectory;
    }

    public boolean release() {
      return release;
    }

    public void release(final boolean release) {
      this.release = release;
    }

    public int retries() {
      return retries;
    }

    public void retries(final int retries) {
      this.retries = retries;
    }

    public TemporaryPorts build() {
      return new TemporaryPorts(this);
    }
  }
}
