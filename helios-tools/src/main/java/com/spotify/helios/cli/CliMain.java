/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.cli;

import com.google.common.collect.Maps;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import com.spotify.helios.auth.AuthProvider;
import com.spotify.helios.auth.AuthProviderSelector;
import com.spotify.helios.auth.ClientAuthenticationPlugin;
import com.spotify.helios.auth.ClientAuthenticationPluginLoader;
import com.spotify.helios.common.LoggingConfig;
import com.spotify.helios.client.RequestDispatcher;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Map;

import static ch.qos.logback.classic.Level.ALL;
import static ch.qos.logback.classic.Level.DEBUG;
import static ch.qos.logback.classic.Level.INFO;
import static ch.qos.logback.classic.Level.WARN;
import static com.google.common.collect.Iterables.get;
import static java.util.Arrays.asList;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;

/**
 * Instantiates and runs helios CLI.
 */
public class CliMain {

  private final CliParser parser;
  private final PrintStream out;
  private final PrintStream err;
  private final AuthProvider.Factory authProviderFactory;

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static void main(final String... args) {
    try {
      int exitCode = new CliMain(System.out, System.err, args).run();
      System.exit(exitCode);
    } catch (Throwable e) {
      // don't print error message for arg parser exception, because parser will do that
      if (!(e instanceof ArgumentParserException)) {
        System.err.println(e.toString());
      }
      System.exit(1);
    }
  }

  public CliMain(final PrintStream out, final PrintStream err, final String... args)
      throws Exception {
    this.authProviderFactory = configureAuthProvider();
    this.parser = new CliParser(args);
    this.out = out;
    this.err = err;
    setupLogging();
  }

  private static AuthProvider.Factory configureAuthProvider() {
    final Map<String, AuthProvider.Factory> authProviderFactories = Maps.newHashMap();

    for (final ClientAuthenticationPlugin plugin : ClientAuthenticationPluginLoader.loadAll()) {
      final AuthProvider.Factory factory = plugin.authProviderFactory();
      if (factory != null) {
        authProviderFactories.put(plugin.schemeName(), factory);
      }
    }

    return new AuthProvider.Factory() {
      @Override
      public AuthProvider create(final RequestDispatcher requestDispatcher) {
        return new AuthProviderSelector(requestDispatcher, authProviderFactories);
      }
    };
  }

  public int run() {
    try {
      final BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
      return parser.getCommand().run(parser.getNamespace(), parser.getTargets(), out, err,
                                     parser.getUsername(), parser.getJson(), stdin,
                                     authProviderFactory);
    } catch (Exception e) {
      // print entire stack trace in verbose mode, otherwise just the exception message
      if (parser.getNamespace().getInt("verbose") > 0) {
        e.printStackTrace(err);
      } else {
        err.println(e.getMessage());
      }
      return 1;
    }
  }

  private void setupLogging() {
    final LoggingConfig config = parser.getLoggingConfig();
    if (config.getNoLogSetup()) {
      return;
    }
    final int verbose = config.getVerbosity();
    final Level level = get(asList(WARN, INFO, DEBUG, ALL), verbose, ALL);
    final Logger rootLogger = (Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME);
    rootLogger.setLevel(level);
  }
}
