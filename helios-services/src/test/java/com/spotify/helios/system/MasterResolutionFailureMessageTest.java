/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.spotify.helios.cli.CliMain;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertTrue;

public class MasterResolutionFailureMessageTest {

  @Test
  public void test() throws Exception {
    final String[] commands = {"job", "list", "--no-log-setup", "-s", "bogussite"};
    final ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
    new CliMain(new PrintStream(stdout), new PrintStream(stderr), commands).run();
    final String string = stderr.toString();
    assertTrue(string.trim().equals("Failed to resolve helios master in bogussite (srv: helios)"));
  }
}
