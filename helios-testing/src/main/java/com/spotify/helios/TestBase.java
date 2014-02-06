/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios;

import com.google.common.base.Strings;

import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static java.lang.System.out;

public class TestBase {

  @Rule
  public TestRule watcher = new TestWatcher() {
    protected void starting(Description description) {
      out.println(Strings.repeat("=", 80));
      out.printf("STARTING: %s: %s%n", description.getClassName(), description.getMethodName());
      out.println(Strings.repeat("=", 80));
    }

    @Override
    protected void succeeded(final Description description) {
      out.println(Strings.repeat("=", 80));
      out.printf("FINISHED: %s: %s%n", description.getClassName(), description.getMethodName());
      out.println(Strings.repeat("=", 80));
    }

    @Override
    protected void failed(final Throwable e, final Description description) {
      out.println(Strings.repeat("=", 80));
      out.printf("FAILED  : %s %s%n", description.getClassName(), description.getMethodName());
      out.println(Strings.repeat("=", 80));
    }
  };
}
