/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios;

import com.google.common.base.Strings;

import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBase {

  static final Logger log = LoggerFactory.getLogger(TestBase.class);

  public TestRule watcher = new TestWatcher() {
    protected void starting(Description description) {
      log.info(Strings.repeat("=", 80));
      log.info("STARTING: {}: {}", description.getClassName(), description.getMethodName());
      log.info(Strings.repeat("=", 80));
    }

    @Override
    protected void succeeded(final Description description) {
      log.info(Strings.repeat("=", 80));
      log.info("FINISHED: {}: {}", description.getClassName(), description.getMethodName());
      log.info(Strings.repeat("=", 80));
    }

    @Override
    protected void failed(final Throwable e, final Description description) {
      log.info(Strings.repeat("=", 80));
      log.info("FAILED  : {} {}", description.getClassName(), description.getMethodName());
      log.info(Strings.repeat("=", 80));
    }
  };
}
