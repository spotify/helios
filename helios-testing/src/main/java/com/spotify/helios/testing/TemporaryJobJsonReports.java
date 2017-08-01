/*-
 * -\-\-
 * Helios Testing Library
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.testing;

import static java.lang.String.format;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.spotify.helios.common.Json;
import com.spotify.helios.testing.descriptors.TemporaryJobEvent;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Map;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports a stream of TemporaryJobEvent events to a JSON file.
 */
public class TemporaryJobJsonReports extends TemporaryJobReports {

  private static final Logger log = LoggerFactory.getLogger(TemporaryJobJsonReports.class);

  private final Path outputDir;

  public TemporaryJobJsonReports(final Path outputDir) {
    this.outputDir = outputDir;
    final File path = this.outputDir.toFile();
    if (!path.mkdirs() && !path.isDirectory()) {
      throw new IllegalStateException(format("failed to create directory \"%s\"", outputDir));
    }
  }

  public ReportWriter getWriterForTest(final Description testDescription) {
    return new JsonReportWriter(
        outputDir, testDescription.getClassName(), testDescription.getMethodName());
  }

  public ReportWriter getDefaultWriter() {
    return new JsonReportWriter(System.err);
  }

  private static class JsonReportWriter extends ReportWriter {

    private final JsonGenerator jg;

    private final String testClassName;
    private final String testName;

    private JsonReportWriter(
        final Path outputDir, final String testClassName, final String testName) {
      this.testClassName = testClassName;
      this.testName = testName;

      final String logFilename = format("%s.%s.json", testClassName, testName).replace('$', '_');
      final File logFile = outputDir.resolve(logFilename).toFile();

      JsonGenerator jg = null;
      try {
        jg = new JsonFactory().createGenerator(logFile, JsonEncoding.UTF8);
        jg.writeStartArray();
      } catch (IOException e) {
        log.error("exception creating event log: {} - {}", logFile.getAbsolutePath(), e);
      }
      this.jg = jg;
    }

    private JsonReportWriter(final OutputStream outputStream) {
      this.testClassName = null;
      this.testName = null;

      JsonGenerator jg = null;
      try {
        jg = new JsonFactory().createGenerator(outputStream, JsonEncoding.UTF8);
      } catch (IOException e) {
        log.error("exception creating event log: {}", e);
      }
      this.jg = jg;
    }

    public Step step(final String step) {
      return new Step(this, step);
    }

    protected void writeEvent(final String step, final double timestamp, final double duration,
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
      if (jg == null) {
        return;
      }

      try {
        Json.writer().writeValue(jg, event);
      } catch (IOException e) {
        log.error("exception writing event to log: {} - {}", event, e);
      }
    }

    @Override
    public void close() throws IOException {
      if (jg != null) {
        jg.close();
      }
    }
  }
}
