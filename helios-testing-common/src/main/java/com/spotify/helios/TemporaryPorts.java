/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public class TemporaryPorts extends ExternalResource {

  // Keep port locks for the duration of the process by default to avoid e.g. failing tests where
  // ports are still in use after the test to affect subsequent tests.
  private static final boolean DEFAULT_RELEASE = false;

  private static final int DEFAULT_RETRIES = 100;
  private static final Path DEFAULT_LOCK_DIRECTORY = Paths.get("/tmp/helios-test/ports/");

  private static final Logger log = LoggerFactory.getLogger(TemporaryPorts.class);

  private final boolean release;
  private final int retries;
  private final Path lockDirectory;

  private final List<AllocatedPort> ports = Lists.newArrayList();

  private volatile boolean closed;

  public TemporaryPorts() {
    this(DEFAULT_RELEASE);
  }

  public TemporaryPorts(final boolean release) {
    this(release, DEFAULT_RETRIES);
  }

  public TemporaryPorts(final boolean release, final int retries) {
    this(release, retries, DEFAULT_LOCK_DIRECTORY);
  }

  public TemporaryPorts(final boolean release, final int retries, final Path lockDirectory) {
    this.release = release;
    this.retries = retries;
    this.lockDirectory = lockDirectory;
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

  public synchronized int localPort(final String name) {
    Preconditions.checkState(!closed, "closed");
    for (int i = 0; i < retries; i++) {
      final int port = ThreadLocalRandom.current().nextInt(40000, 49152);
      final AllocatedPort allocatedPort = lock(port, name);
      if (allocatedPort == null) {
        continue;
      }
      log.debug("allocated port \"{}\": {}", name, port);
      ports.add(allocatedPort);
      return port;
    }
    throw new AllocationFailedException();
  }

  public synchronized Range<Integer> localPortRange(final String name, final int n) {
    Preconditions.checkState(!closed, "closed");
    for (int i = 0; i < retries; i++) {
      final int base = ThreadLocalRandom.current().nextInt(40000, 49152);
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
      return new AllocatedPort(path, file, lock);
    } catch (OverlappingFileLockException e) {
      return null;
    } catch (IOException e) {
      log.error("Failed to take port lock: {}", path, e);
      throw Throwables.propagate(e);
    }
  }

  private static class AllocatedPort {

    private Path path;
    private final FileChannel file;
    private final FileLock lock;

    private AllocatedPort(final Path path, FileChannel file, FileLock lock) {
      this.path = path;
      this.file = file;
      this.lock = lock;
    }

    void release() {
      try {
        lock.release();
      } catch (Exception e) {
        log.error("caught exception releasing port lock: {}", path, e);
      }
      try {
        file.close();
      } catch (Exception e) {
        log.error("caught exception closing port lock file: {}", path, e);
      }
      try {
        Files.deleteIfExists(path);
      } catch (Exception e) {
        log.error("caught exception deleting port lock file: {}", path, e);
      }
    }
  }

  public class AllocationFailedException extends RuntimeException {
  }
}
