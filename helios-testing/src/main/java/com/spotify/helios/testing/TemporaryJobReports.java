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

import com.google.common.collect.Maps;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.junit.runner.Description;

/**
 * Reports a stream of TemporaryJobEvent events.
 */
public abstract class TemporaryJobReports {

  public abstract ReportWriter getWriterForTest(final Description testDescription);

  public abstract ReportWriter getDefaultWriter();

  public abstract static class ReportWriter implements Closeable {
    public Step step(final String step) {
      return new Step(this, step);
    }

    protected abstract void writeEvent(
        final String step, final double timestamp, final double duration, final Boolean success,
        final Map<String, Object> tags);

    @Override
    public void close() throws IOException {
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
      final long durationMillis = System.currentTimeMillis() - timestampMillis;
      final double timestamp = timestampMillis / 1000.;
      final double duration = durationMillis / 1000.;

      reportWriter.writeEvent(step, timestamp, duration, success, tags);
    }
  }
}
