/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

public class BadImageNameTest extends SystemTestBase {

  // TODO (dano): this test doesn't need all of SystemTestBase

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    exception.expect(IllegalArgumentException.class);
    createJob(JOB_NAME, JOB_VERSION, "DOES_NOT_LIKE_AT_ALL-CAPITALS",
              ImmutableList.of("/bin/true"));
  }
}
