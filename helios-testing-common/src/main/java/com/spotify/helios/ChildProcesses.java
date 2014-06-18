/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

public class ChildProcesses {

  private static final Logger log = LoggerFactory.getLogger(ChildProcesses.class);

  public static Process spawn(final Class<?> clazz, final String... args) throws IOException {
    final String main = clazz.getName();
    return spawn(main, args);
  }

  public static Process spawn(final String main, final String[] args) throws IOException {
    final String java = System.getProperty("java.home") + "/bin/java";
    final String classpath = System.getProperty("java.class.path");
    final List<String> cmd = ImmutableList.<String>builder()
        .add(java, "-cp", classpath,
             "-Xms64m", "-Xmx64m",
             "-XX:+TieredCompilation",
             "-XX:TieredStopAtLevel=1")
        .add(main)
        .add(String.valueOf(pid()))
        .add(args)
        .build();
    final Process process = new ProcessBuilder()
        .command(cmd)
        .start();
    monitor(main, process);
    return process;
  }

  private static void monitor(final String main, final Process process) {
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        while (true) {
          try {
            final int exitCode = process.waitFor();
            if (exitCode != 0) {
              log.warn("{} exited: {}", main, exitCode);
            } else {
              log.info("{} exited: 0");
            }
            return;
          } catch (InterruptedException ignored) {
          }
        }
      }
    });
  }

  private static int pid() {
    final String name = ManagementFactory.getRuntimeMXBean().getName();
    final String[] parts = name.split("@");
    return Integer.valueOf(parts[0]);
  }

  public abstract static class Child {

    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    public void run(String[] args) throws Exception {
      if (args.length < 1) {
        System.err.println("invalid arguments: " + Arrays.toString(args));
        System.exit(2);
        return;
      }
      final int parent = Integer.valueOf(args[0]);
      try {
        Executors.newSingleThreadExecutor().execute(new Runnable() {
          @Override
          public void run() {
            monitorParent(parent);
          }
        });
        start(args);
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(3);
      }
    }

    protected abstract void start(final String[] args) throws Exception;

    private void monitorParent(final int pid) {
      while (true) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          continue;
        }
        final String[] cmd = {"ps", "-p", String.valueOf(pid)};
        try {
          final int exitCode = Runtime.getRuntime().exec(cmd).waitFor();
          if (exitCode == 1) {
            System.exit(0);
          }
        } catch (InterruptedException ignored) {
        } catch (IOException e) {
          System.exit(0);
        }
      }
    }
  }
}
