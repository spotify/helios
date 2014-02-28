/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.util.concurrent.Service;

import com.spotify.helios.ZooKeeperStandaloneServerManager;
import com.spotify.helios.agent.AgentMain;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.file.Files;
import java.nio.file.Path;

public class AgentStateDirConflictTest {
  @Rule
  public ExpectedException exception = ExpectedException.none();

  private Thread.UncaughtExceptionHandler dueh;
  private Path stateDir;
  private AgentMain first;
  private AgentMain second;
  private ZooKeeperStandaloneServerManager zk;

  @Before
  public void setup() throws Exception {
    zk = new ZooKeeperStandaloneServerManager();
    dueh = Thread.getDefaultUncaughtExceptionHandler();

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(final Thread t, final Throwable e) {}
    });

    stateDir = Files.createTempDirectory("helios-agent-conflict-test");

    first = makeAgent("first");
    second = makeAgent("second");
  }

  private AgentMain makeAgent(String name) throws ArgumentParserException {
    return new AgentMain("-vvvv",
                        "--no-log-setup",
                        "--no-http",
                        "--name=" + name,
                        "--zk=" + zk.connectString(),
                        "--state-dir", stateDir.toString());
  }

  @After
  public void teardown() {
    FileUtils.deleteQuietly(stateDir.toFile());
    stopQuietly(first);
    stopQuietly(second);
    Thread.setDefaultUncaughtExceptionHandler(dueh);
    zk.stop();
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
    first.startAsync().awaitRunning();
    exception.expect(IllegalStateException.class);
    second.startAsync().awaitRunning();
  }
}
