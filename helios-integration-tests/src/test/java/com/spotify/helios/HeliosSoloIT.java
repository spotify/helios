/*-
 * -\-\-
 * Helios Integration Tests
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

package com.spotify.helios;

import static com.spotify.helios.system.SystemTestBase.ALPINE;
import static com.spotify.helios.system.SystemTestBase.NGINX;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import com.google.common.net.HostAndPort;
import com.spotify.helios.testing.HeliosDeploymentResource;
import com.spotify.helios.testing.HeliosSoloDeployment;
import com.spotify.helios.testing.InMemoryLogStreamFollower;
import com.spotify.helios.testing.TemporaryJob;
import com.spotify.helios.testing.TemporaryJobs;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.awaitility.Awaitility;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@SuppressWarnings("AbbreviationAsWordInName")
public class HeliosSoloIT {

  @Rule
  public final ExpectedException expected = ExpectedException.none();

  private static final InMemoryLogStreamFollower logStreamProvider =
      InMemoryLogStreamFollower.create();

  @ClassRule
  public static HeliosDeploymentResource solo = new HeliosDeploymentResource(
      HeliosSoloDeployment.fromEnv()
          .heliosSoloImage(Utils.soloImage())
          .checkForNewImages(false)
          .removeHeliosSoloOnExit(false)
          .logStreamProvider(logStreamProvider)
          .env("REGISTRAR_HOST_FORMAT", "_${service}._${protocol}.test.${domain}")
          .env("WHITELISTED_CAPS", "IPC_LOCK,SYSLOG")
          .build()
  );

  @Rule public final TemporaryPorts ports = TemporaryPorts.create();

  @Rule
  public TemporaryJobs jobs = TemporaryJobs.builder()
      .client(solo.client())
      .deployTimeoutMillis(MINUTES.toMillis(1))
      .hostFilter(".+")
      .build();


  @Test
  public void testHttpHealthcheck() {
    jobs.job()
        .image("nginx:1.9.9")
        .port("http", 80)
        .httpHealthCheck("http", "/")
        .deploy();
  }

  @Test
  public void testTcpHealthcheck() {
    jobs.job()
        .image("nginx:1.9.9")
        .port("http", 80)
        .tcpHealthCheck("http")
        .deploy();
  }

  @Test
  public void testServiceDiscovery() throws Exception {
    // start a container that runs nginx and registers with SkyDNS
    final TemporaryJob nginx = jobs.job()
        .image(NGINX)
        .port("http", 80, ports.localPort("http"))
        .registration("nginx", "http", "http")
        .deploy();

    // run a container that does SRV lookup to find the nginx service and then curl's it
    final TemporaryJob alpine = jobs.job()
        .image(ALPINE)
        .port("nc", 4711, ports.localPort("nc"))
        .command("sh", "-c",
            "apk add --update bind-tools "
            + "&& export SRV=$(dig -t SRV +short _nginx._http.test.$SPOTIFY_DOMAIN) "
            + "&& export HOST=$(echo $SRV | cut -d' ' -f4) "
            + "&& export PORT=$(echo $SRV | cut -d' ' -f3) "
            + "&& nc -lk -p 4711 -e curl http://$HOST:$PORT"
        )
        .deploy();

    final HostAndPort alpineAddress = alpine.address("nc");

    // Connect to alpine container to get the curl response. If we get back the nginx welcome page
    // we know that helios properly registered the nginx service in SkyDNS.
    final Callable<String> socketResponse = () -> {
      try (final Socket s = new Socket(alpineAddress.getHostText(), alpineAddress.getPort())) {
        return IOUtils.toString(s.getInputStream()).trim();
      }
    };
    // allow a few retries for a delay in the apk install of bind-tools
    Awaitility.await("alpine container returns nginx welcome")
        .atMost(10, TimeUnit.SECONDS)
        .until(socketResponse, containsString("Welcome to nginx!"));

    // also throw in a check to make sure log streaming is working
    assertThat(new String(logStreamProvider.getStderr(nginx.job().getId())),
        containsString("nginx"));
  }

  @Test
  public void testWhitelistCaps() throws Exception {
    jobs.job()
        .image("nginx:1.9.9")
        .addCapabilities(asList("IPC_LOCK", "SYSLOG"))
        .deploy();

    // NET_RAW is not whitelisted so creating this job should fail
    expected.expect(AssertionError.class);
    jobs.job()
        .image("nginx:1.9.9")
        .addCapabilities(singletonList("NET_RAW"))
        .deploy();
  }
}
