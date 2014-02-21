/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.util.concurrent.Service;

import com.spotify.helios.agent.AgentMain;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

public class AgentStateDirConflictTest {

  static final Logger log = LoggerFactory.getLogger(AgentStateDirConflictTest.class);

  Thread.UncaughtExceptionHandler dueh;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  Path stateDir;

  AgentMain foo;
  AgentMain bar;

  @Before
  public void setup() throws Exception {
    dueh = Thread.getDefaultUncaughtExceptionHandler();

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(final Thread t, final Throwable e) {
      }
    });

    stateDir = Files.createTempDirectory("helios-agent-conflict-test");

    foo = new AgentMain("-vvvv",
                        "--no-log-setup",
                        "--no-http",
                        "--name=foo",
                        "--state-dir", stateDir.toString());

    bar = new AgentMain("-vvvv",
                        "--no-log-setup",
                        "--no-http",
                        "--name=bar",
                        "--state-dir", stateDir.toString());
  }

  @After
  public void teardown() {
    FileUtils.deleteQuietly(stateDir.toFile());
    stopQuietly(foo);
    stopQuietly(bar);
    Thread.setDefaultUncaughtExceptionHandler(dueh);
  }

  private void stopQuietly(final Service service) {
    if (service == null) {
      return;
    }
    try {
      service.stopAsync().awaitTerminated();
    } catch (Exception ignore) {
    }
  }

  @Test
  public void test() throws Exception {
    foo.startAsync().awaitRunning();
    exception.expect(IllegalStateException.class);
    bar.startAsync().awaitRunning();
  }
}
