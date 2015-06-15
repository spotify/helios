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

package com.spotify.helios.testing;

import com.google.common.collect.Maps;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.spotify.helios.common.Json;
import com.spotify.helios.testing.descriptors.TemporaryJobEvent;

import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static java.lang.String.format;

/**
 * Reports a stream of TemporaryJobEvent events to a JSON file.
 */
public class TemporaryJobReports {
  private static final Logger log = LoggerFactory.getLogger(TemporaryJobReports.class);

  private final Path outputDir;

  TemporaryJobReports(final Path outputDir) {
    this.outputDir = outputDir;
    this.outputDir.toFile().mkdirs();
  }

  public ReportWriter getWriterForTest(final Description testDescription) throws IOException {
    return new ReportWriter(outputDir, testDescription.getClassName(),
                            testDescription.getMethodName());
  }

  public static class ReportWriter implements Closeable {

    private final JsonGenerator jg;

    private final String testClassName;
    private final String testName;

    private ReportWriter(final Path outputDir, final String testClassName, final String testName)
      throws IOException {
      this.testClassName = testClassName;
      this.testName = testName;

      final String logFilename = format("%s.%s.json", testClassName, testName).replace('$', '_');
      final File logFile = outputDir.resolve(logFilename).toFile();

      jg = new JsonFactory().createGenerator(logFile, JsonEncoding.UTF8);
      jg.writeStartArray();
    }

    public Step step(final String step) {
      return new Step(this, step);
    }

    private void writeEvent(final String step, final double timestamp, final double duration,
                            final Boolean success, final Map<String, Object> tags) {
      final TemporaryJobEvent event = new TemporaryJobEvent(
          timestamp,
          duration,
          testClassName,
          testName,
          step,
          success,
          tags
      );

      writeEvent(event);
    }

    private void writeEvent(final TemporaryJobEvent event) {
      try {
        Json.writer().writeValue(jg, event);
      } catch (IOException e) {
        log.error("exception writing event to log: {} - {}", event, e);
      }
    }

    @Override
    public void close() throws IOException {
      jg.close();
    }

  }

  public static class Step {

    private final ReportWriter reportWriter;
    private final String step;
    private final long timestampMillis;
    private final Map<String, Object> tags;

    private boolean success = false;

    Step(final ReportWriter reportWriter, final String step) {
      this.reportWriter = reportWriter;
      this.step = step;
      this.timestampMillis = System.currentTimeMillis();
      this.tags = Maps.newHashMap();
    }

    public Step tag(final String key, final Object value) {
      tags.put(key, value);
      return this;
    }

    public Step markSuccess() {
      this.success = true;
      return this;
    }

    public void finish() {
      final long durationMillis = System.currentTimeMillis() -  timestampMillis;
      final double timestamp = timestampMillis / 1000.;
      final double duration = durationMillis / 1000.;

      reportWriter.writeEvent(step, timestamp, duration, success, tags);
    }
  }
}
