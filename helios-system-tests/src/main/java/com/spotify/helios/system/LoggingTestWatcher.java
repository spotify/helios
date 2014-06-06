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

package com.spotify.helios.system;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

final class LoggingTestWatcher extends TestWatcher {
  private static final Logger log = LoggerFactory.getLogger(LoggingTestWatcher.class);

  @Override
  protected void starting(Description description) {
    if (Boolean.getBoolean("logToFile")) {
      final String name = description.getClassName() + "_" + description.getMethodName();
      setupFileLogging(name);
    }
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
    log.info("Exception", e);
    log.info(Strings.repeat("=", 80));
  }

  private void setupFileLogging(final String name) {
    final ch.qos.logback.classic.Logger rootLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME);
    final LoggerContext context = rootLogger.getLoggerContext();
    context.reset();
    final FileAppender<ILoggingEvent> fileAppender = new FileAppender<>();
    final String ts = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSS").format(new Date());
    final String pid = ManagementFactory.getRuntimeMXBean().getName().split("@", 2)[0];
    final Path directory = Paths.get(System.getProperty("logDir", "/tmp/helios-test/log/"));
    final String filename = String.format("%s-%s-%s.log", ts, name, pid);
    final Path file = directory.resolve(filename);
    final PatternLayoutEncoder ple = new PatternLayoutEncoder();
    ple.setContext(context);
    ple.setPattern("%d{HH:mm:ss.SSS} %-5level %logger{1} %F:%L - %msg%n");
    ple.start();
    fileAppender.setEncoder(ple);
    fileAppender.setFile(file.toString());
    fileAppender.setContext(context);
    fileAppender.start();
    rootLogger.setLevel(Level.DEBUG);
    rootLogger.addAppender(fileAppender);
    try {
      Files.createDirectories(directory);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    configureLogger("org.eclipse.jetty", Level.ERROR);
    configureLogger("org.apache.curator", Level.ERROR);
    configureLogger("org.apache.zookeeper", Level.ERROR);
    configureLogger("com.yammer.metrics", Level.ERROR);
    configureLogger("com.spotify.helios", Level.DEBUG);
  }

  private void configureLogger(final String name, final Level level) {
    final ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(name);
    logger.setLevel(level);
  }
}
