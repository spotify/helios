/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import org.junit.Test;

public class AgentStateDirConflictTest extends SystemTestBase {

  // TODO (dano): this test doesn't need all of SystemTestBase

  @Test
  public void test() throws Exception {
    startDefaultAgent(TEST_AGENT).awaitRunning();
    exception.expect(IllegalStateException.class);
    startDefaultAgent(TEST_AGENT).awaitRunning();
  }
}
