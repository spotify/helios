/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.base.Throwables;

import com.spotify.nameless.Service;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public abstract class NamelessTestBase extends SystemTestBase {

  com.spotify.nameless.Service nameless;
  private FileChannel lockFile;
  private FileLock lock;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    // TODO (dano): remove this when nameless can be configured with a unique port
    final Path lockPath = Paths.get("/tmp/helios-test-nameless-lock");
    try {
      log.debug("Taking nameless lock: {}", lockPath);
      lockFile = FileChannel.open(lockPath, CREATE, WRITE);
      lock = lockFile.lock();
    } catch (OverlappingFileLockException | IOException e) {
      log.error("Failed to take nameless lock: {}", lockPath, e);
      throw Throwables.propagate(e);
    }

    nameless = new Service();
    nameless.start("--no-log-configuration");
  }

  @Override
  @After
  public void teardown() throws Exception {
    try {
      nameless.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (lock != null)  {
      lock.release();
    }
    if (lockFile != null) {
      lockFile.close();
    }
    super.teardown();
  }
}
