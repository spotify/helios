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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;

import com.google.common.base.Optional;
import com.spotify.helios.common.descriptors.HealthCheck;
import com.spotify.helios.common.descriptors.HttpHealthCheck;
import com.spotify.helios.common.descriptors.TcpHealthCheck;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import org.junit.Rule;
import org.junit.Test;

public class HealthCheckTest extends TemporaryJobsTestBase {

  private static final String HEALTH_CHECK_PORT = "healthCheck";
  private static final String QUERY_PORT = "query";

  @Test
  public void test() throws Exception {
    assertThat(testResult(TestImpl.class), isSuccessful());
  }

  public static class TestImpl {

    @Rule
    public final TemporaryJobs temporaryJobs = temporaryJobsBuilder()
        .client(client)
        .jobPrefix(Optional.of(testTag).get())
        .deployTimeoutMillis(MINUTES.toMillis(3))
        .build();

    @Test
    public void testTcpCheck() throws Exception {
      // running netcat twice on different ports lets us verify the health check actually executed
      // because otherwise we wouldn't be able to connect to the second port.
      final TemporaryJob job = temporaryJobs.job()
          .image(ALPINE)
          .command("sh", "-c", "nc -l -p 4711 && nc -kl -p 4712 -e true")
          .port(HEALTH_CHECK_PORT, 4711)
          .port(QUERY_PORT, 4712)
          .tcpHealthCheck(HEALTH_CHECK_PORT)
          .deploy(testHost1);

      // verify health check was set correctly in job
      assertThat(job.job().getHealthCheck(),
          equalTo((HealthCheck) TcpHealthCheck.of(HEALTH_CHECK_PORT)));

      // verify we can actually connect to the port
      // noinspection EmptyTryBlock
      try (final Socket ignored = new Socket(DOCKER_HOST.address(),
          job.address(QUERY_PORT).getPort())) {
        // ignored
      }
    }

    @Test
    public void testHttpCheck() throws Exception {
      // Start an HTTP server that listens on ports 4711 and 4712.
      final TemporaryJob job = temporaryJobs.job()
          .image(UHTTPD)
          .command("-p", "4711", "-p", "4712")
          .port(HEALTH_CHECK_PORT, 4711)
          .port(QUERY_PORT, 4712)
          .httpHealthCheck(HEALTH_CHECK_PORT, "/")
          .deploy(testHost1);

      // verify health check was set correctly in job
      assertThat(job.job().getHealthCheck(),
          equalTo((HealthCheck) HttpHealthCheck.of(HEALTH_CHECK_PORT, "/")));

      // verify we can actually make http requests
      final URL url = new URL("http", DOCKER_HOST.address(),
          job.address(QUERY_PORT).getPort(), "/");
      final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      assertThat(connection.getResponseCode(), equalTo(200));
    }

    @Test
    public void testHealthCheck() throws Exception {
      // same as the tcp test above, but uses a HealthCheck
      // object instead of the tcpHealthCheck convenience method
      final HealthCheck healthCheck = TcpHealthCheck.of(HEALTH_CHECK_PORT);
      final TemporaryJob job = temporaryJobs.job()
          .image(ALPINE)
          .command("sh", "-c", "nc -l -p 4711 && nc -kl -p 4712 -e true")
          .port(HEALTH_CHECK_PORT, 4711)
          .port(QUERY_PORT, 4712)
          .healthCheck(healthCheck)
          .deploy(testHost1);

      // verify health check was set correctly in job
      assertThat(job.job().getHealthCheck(), equalTo(healthCheck));

      // verify we can actually connect to the port
      // noinspection EmptyTryBlock
      try (final Socket ignored = new Socket(DOCKER_HOST.address(),
          job.address(QUERY_PORT).getPort())) {
        // ignored
      }
    }
  }

}
