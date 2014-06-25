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
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public class ChildProcesses {

  private static final Logger log = LoggerFactory.getLogger(ChildProcesses.class);

  public static final int OK_EXIT_CODE = 0;
  public static final int INVALID_ARGUMENTS_EXIT_CODE = 2;
  public static final int EXCEPTION_EXIT_CODE = 3;
  public static final int CHILD_EXIT_CODE = 4;

  public static SubprocessBuilder process() {
    return new SubprocessBuilder();
  }

  private static void monitor(final String main, final Subprocess process,
                              final Integer exitParentOnChildExit) {
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        while (true) {
          try {
            final int exitCode = process.join();
            if (process.killed()) {
              return;
            }
            if (exitParentOnChildExit != null) {
              log.error("{} exited: {}. Exiting.", exitCode);
              System.exit(exitParentOnChildExit);
            } else if (exitCode != 0) {
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
        System.exit(INVALID_ARGUMENTS_EXIT_CODE);
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
      } catch (Throwable e) {
        e.printStackTrace();
        System.exit(EXCEPTION_EXIT_CODE);
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
            System.exit(OK_EXIT_CODE);
          }
        } catch (InterruptedException ignored) {
        } catch (IOException e) {
          System.exit(OK_EXIT_CODE);
        }
      }
    }
  }

  public static class SubprocessBuilder {

    private Integer parentExitCodeOnChildExit;
    private String main;
    private List<String> args;

    public Subprocess spawn() throws IOException {
      final String java = System.getProperty("java.home") + "/bin/java";
      final String classpath = System.getProperty("java.class.path");
      final List<String> cmd = ImmutableList.<String>builder()
          .add(java, "-cp", classpath,
               "-Xms64m", "-Xmx64m",
               "-XX:+TieredCompilation",
               "-XX:TieredStopAtLevel=1")
          .add(main)
          .add(String.valueOf(pid()))
          .addAll(args)
          .build();
      final Process process = new ProcessBuilder()
          .command(cmd)
          .start();
      Subprocess subprocess = new Subprocess(process);
      monitor(main, subprocess, parentExitCodeOnChildExit);
      return subprocess;
    }


    public SubprocessBuilder exitParentOnChildExit(final int exitCode) {
      this.parentExitCodeOnChildExit = exitCode;
      return this;
    }

    public SubprocessBuilder exitParentOnChildExit() {
      return exitParentOnChildExit(CHILD_EXIT_CODE);
    }

    public SubprocessBuilder main(final Class<?> main) {
      checkNotNull(findMain(main), "class %s does not have a main method", main);
      return main(main.getName());
    }

    private SubprocessBuilder main(final String main) {
      this.main = main;
      return this;
    }

    private Method findMain(final Class<?> cls) {
      for (final Method method : cls.getMethods()) {
        int mod = method.getModifiers();
        if (method.getName().equals("main") &&
            Modifier.isPublic(mod) && Modifier.isStatic(mod) &&
            method.getParameterTypes().length == 1 &&
            method.getParameterTypes()[0].equals(String[].class)) {
          return method;
        }
      }
      return null;
    }

    public SubprocessBuilder args(final String... args) {
      return args(asList(args));
    }

    private SubprocessBuilder args(final List<String> args) {
      this.args = ImmutableList.copyOf(args);
      return this;
    }
  }

  public static class Subprocess {

    private final AtomicBoolean killed = new AtomicBoolean();

    private final Process process;

    public Subprocess(final Process process) {
      this.process = process;
    }

    public void kill() {
      killed.set(true);
      // TODO (dano): send SIGKILL after a timeout if process doesn't exit on SIGTERM.
      process.destroy();
    }

    public boolean killed() {
      return killed.get();
    }

    public int join() throws InterruptedException {
      return process.waitFor();
    }

    public boolean running() {
      try {
        process.exitValue();
        return false;
      } catch (IllegalThreadStateException e) {
        return true;
      }
    }
  }
}
