/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.spotify.nameless.Service;

import org.junit.After;
import org.junit.Before;

public class NamelessTestBase extends SystemTestBase {

  com.spotify.nameless.Service nameless;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    nameless = new Service();
    nameless.start();
  }

  @Override
  @After
  public void teardown() throws Exception {
    try {
      nameless.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
    super.teardown();
  }
}
