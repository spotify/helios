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

package com.spotify.helios;

import com.google.common.net.HostAndPort;

import com.spotify.helios.testing.TemporaryJob;
import com.spotify.helios.testing.TemporaryJobs;

import org.apache.commons.io.IOUtils;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.Socket;

import static com.spotify.helios.system.SystemTestBase.ALPINE;
import static com.spotify.helios.system.SystemTestBase.NGINX;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public class HeliosSoloIT {

  @ClassRule
  public static final TemporaryPorts PORTS = TemporaryPorts.create();

  @ClassRule
  public static final TemporaryJobs JOBS = TemporaryJobs.builder()
      .deployTimeoutMillis(MINUTES.toMillis(1))
      .hostFilter(".+")
      .build();

  @Test
  public void testHttpHealthcheck() {
    JOBS.job()
        .image("nginx:1.9.9")
        .port("http", 80)
        .httpHealthCheck("http", "/")
        .deploy();
  }

  @Test
  public void testTcpHealthcheck() {
    JOBS.job()
        .image("nginx:1.9.9")
        .port("http", 80)
        .tcpHealthCheck("http")
        .deploy();
  }

  @Test
  public void testServiceDiscovery() throws Exception {
    // start a container that runs nginx and registers with SkyDNS
    JOBS.job()
        .image(NGINX)
        .port("http", 80, PORTS.localPort("http"))
        .registration("nginx", "http", "http")
        .deploy();

    // run a container that does SRV lookup to find the nginx service and then curl's it
    final TemporaryJob alpine = JOBS.job()
        .image(ALPINE)
        .port("nc", 4711, PORTS.localPort("nc"))
        .command("sh", "-c",
                 "apk-install bind-tools " +
                 // TODO (dxia) Should we let users set env vars for HeliosSoloDeployment
                 // like REGISTRAR_HOST_FORMAT that controls service discovery? If so, how?
                 // By passing in a Config to a TemporaryJobs static factory method?
                 "&& export SRV=$(dig -t SRV +short _nginx._http.services.$SPOTIFY_DOMAIN) " +
                 "&& export HOST=$(echo $SRV | cut -d' ' -f4) " +
                 "&& export PORT=$(echo $SRV | cut -d' ' -f3) " +
                 "&& nc -lk -p 4711 -e curl http://$HOST:$PORT"
        )
        .deploy();

    final HostAndPort alpineAddress = alpine.address("nc");

    // Connect to alpine container to get the curl response. If we get back the nginx welcome page
    // we know that helios properly registered the nginx service in SkyDNS.
    try (final Socket s = new Socket(alpineAddress.getHostText(), alpineAddress.getPort())) {
      final String result = IOUtils.toString(s.getInputStream()).trim();
      assertThat(result, containsString("Welcome to nginx!"));
    }
  }
}
