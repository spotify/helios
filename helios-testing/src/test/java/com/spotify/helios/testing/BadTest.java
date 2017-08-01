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

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.hasFailureContaining;

import com.google.common.base.Optional;
import org.junit.Rule;
import org.junit.Test;


public class BadTest extends TemporaryJobsTestBase {

  @Test
  public void verifyJobFailsWhenCalledBeforeTestRun() throws Exception {
    assertThat(testResult(BadTestImpl.class),
        hasFailureContaining("deploy() must be called in a @Before or in the test method"));
  }

  public static class BadTestImpl {

    @Rule
    public final TemporaryJobs temporaryJobs = temporaryJobsBuilder()
        .client(client)
        .prober(new TestProber())
        .jobPrefix(Optional.of(testTag).get())
        .build();

    @SuppressWarnings("unused")
    private TemporaryJob job2 = temporaryJobs.job()
        .image("base")
        .deploy(testHost1);

    @Test
    public void testFail() throws Exception {
      fail();
    }
  }

}
