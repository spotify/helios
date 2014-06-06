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

package com.spotify.helios.servicescommon.logging;

import net.kencochrane.raven.logback.SentryAppender;

import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.management.ManagementFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.net.SyslogAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

import static ch.qos.logback.classic.Level.OFF;

public class LoggingConfigurator {

  public static final String DEFAULT_IDENT = "helios";

  public enum Level {
    OFF(ch.qos.logback.classic.Level.OFF),
    ERROR(ch.qos.logback.classic.Level.ERROR),
    WARN(ch.qos.logback.classic.Level.WARN),
    INFO(ch.qos.logback.classic.Level.INFO),
    DEBUG(ch.qos.logback.classic.Level.DEBUG),
    TRACE(ch.qos.logback.classic.Level.TRACE),
    ALL(ch.qos.logback.classic.Level.ALL);

    final ch.qos.logback.classic.Level logbackLevel;

    Level(ch.qos.logback.classic.Level logbackLevel) {
      this.logbackLevel = logbackLevel;
    }
  }

  /**
   * Mute all logging.
   */
  public static void configureNoLogging() {
    final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    final LoggerContext context = rootLogger.getLoggerContext();

    // Clear context, removing all appenders
    context.reset();

    // Set logging level to OFF
    for (final Logger logger : context.getLoggerList()) {
      if (logger != rootLogger) {
        logger.setLevel(null);
      }
    }
    rootLogger.setLevel(OFF);
  }

  /**
   * Configure logging with default behaviour and log to stderr. Uses the {@link #DEFAULT_IDENT}
   * logging identity. Uses INFO logging level.
   */
  public static void configureDefaults() {
    configureDefaults(DEFAULT_IDENT);
  }

  /**
   * Configure logging with default behaviour and log to stderr using INFO logging level.
   *
   * @param ident The logging identity.
   */
  public static void configureDefaults(final String ident) {
    configureDefaults(ident, Level.INFO);
  }

  /**
   * Configure logging with default behaviour and log to stderr.
   *
   * @param ident The logging identity.
   * @param level logging level to use.
   */
  public static void configureDefaults(final String ident, final Level level) {
    final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    // Setup context
    final LoggerContext context = rootLogger.getLoggerContext();
    context.reset();
    context.putProperty("ident", ident);
    context.putProperty("pid", getMyPid());

    // Setup stderr output
    rootLogger.addAppender(getStdErrAppender(context));

    // Setup logging level
    rootLogger.setLevel(level.logbackLevel);

    // Log uncaught exceptions
    setDefaultUncaughtExceptionHandler(rootLogger);
  }


  /**
   * Configure logging with default behavior and log to syslog using INFO logging level.
   *
   * @param ident Syslog ident to use.
   */
  public static void configureSyslogDefaults(final String ident) {
    configureSyslogDefaults(ident, Level.INFO);
  }


  /**
   * Configure logging with default behavior and log to syslog.
   *
   * @param ident Syslog ident to use.
   * @param level logging level to use.
   */
  public static void configureSyslogDefaults(final String ident, final Level level) {
    final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    // Setup context
    final LoggerContext context = rootLogger.getLoggerContext();
    context.reset();
    context.putProperty("ident", ident);
    context.putProperty("pid", getMyPid());

    // Setup syslog output
    rootLogger.addAppender(getSyslogAppender(context));

    // Setup logging level
    rootLogger.setLevel(level.logbackLevel);

    // Log uncaught exceptions
    setDefaultUncaughtExceptionHandler(rootLogger);
  }

  /**
   * Add a sentry appender for error log event.
   * @param dsn the sentry dsn to use (as produced by the sentry webinterface).
   */
  public static void addSentryAppender(final String dsn) {
    addSentryAppender(dsn, Level.ERROR);
  }

  /**
   * Add a sentry appender.
   * @param dsn the sentry dsn to use (as produced by the sentry webinterface).
   * @param logLevelThreshold the threshold for log events to be sent to sentry.
   */
  public static void addSentryAppender(final String dsn, Level logLevelThreshold) {
    final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    final LoggerContext context = rootLogger.getLoggerContext();

    SentryAppender appender = new SentryAppender();
    appender.setDsn(dsn);

    appender.setContext(context);
    ThresholdFilter levelFilter = new ThresholdFilter();
    levelFilter.setLevel(logLevelThreshold.logbackLevel.toString());
    levelFilter.start();
    appender.addFilter(levelFilter);

    appender.start();

    rootLogger.addAppender(appender);
  }

  /**
   * Create a stderr appender.
   *
   * @param context The logger context to use.
   * @return An appender writing to stderr.
   */
  private static Appender<ILoggingEvent> getStdErrAppender(final LoggerContext context) {

    // Setup format
    final PatternLayoutEncoder encoder = new PatternLayoutEncoder();
    encoder.setContext(context);
    encoder.setPattern("%date{HH:mm:ss.SSS} %property{ident}[%property{pid}]: " +
                       "%-5level [%thread] %logger{0}: %msg%n");
    encoder.start();

    // Setup stderr appender
    final ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<ILoggingEvent>();
    appender.setTarget("System.err");
    appender.setName("stderr");
    appender.setEncoder(encoder);
    appender.setContext(context);
    appender.start();

    return appender;
  }

  /**
   * Create a syslog appender. The appender will use the facility local0.
   *
   * @param context The logger context to use.
   * @return An appender that writes to syslog.
   */
  private static Appender<ILoggingEvent> getSyslogAppender(final LoggerContext context) {
    final SyslogAppender appender = new MillisecondPrecisionSyslogAppender();

    appender.setFacility("LOCAL0");
    appender.setSyslogHost("localhost");
    appender.setName("syslog");
    appender.setContext(context);
    appender.setSuffixPattern(
        "%property{ident}[%property{pid}]: %-5level [%thread] %logger{0} - %msg");
    appender.setStackTracePattern(
        "%property{ident}[%property{pid}]: %-5level [%thread] %logger{0} - " + CoreConstants.TAB);
    appender.start();

    return appender;
  }

  /**
   * Configure logging using a logback configuration file.
   *
   * @param file A logback configuration file.
   */
  public static void configure(final File file) {
    configure(file, DEFAULT_IDENT);
  }

  /**
   * Configure logging using a logback configuration file.
   *
   * @param file         A logback configuration file.
   * @param defaultIdent Fallback logging identity, used if not specified in config file.
   */
  public static void configure(final File file, final String defaultIdent) {
    final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    // Setup context
    final LoggerContext context = rootLogger.getLoggerContext();
    context.reset();

    // Log uncaught exceptions
    setDefaultUncaughtExceptionHandler(rootLogger);

    // Load logging configuration from file
    try {
      final JoranConfigurator configurator = new JoranConfigurator();
      configurator.setContext(context);
      configurator.doConfigure(file);
    } catch (JoranException je) {
      // StatusPrinter will handle this
    }

    context.putProperty("pid", getMyPid());

    final String ident = context.getProperty("ident");
    if (ident == null) {
      context.putProperty("ident", defaultIdent);
    }

    StatusPrinter.printInCaseOfErrorsOrWarnings(context);
  }

  private static void setDefaultUncaughtExceptionHandler(final Logger rootLogger) {
    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(final Thread t, final Throwable e) {
        rootLogger.error("Unchaught exception", e);
      }
    });
  }

  // Also, the portability of this function is not guaranteed.
  private static String getMyPid() {
    String pid = "0";
    try {
      final String nameStr = ManagementFactory.getRuntimeMXBean().getName();

      // XXX (bjorn): Really stupid parsing assuming that nameStr will be of the form
      // "pid@hostname", which is probably not guaranteed.
      pid = nameStr.split("@")[0];
    } catch (RuntimeException e) {
      // Fall through.
    }
    return pid;
  }

}
