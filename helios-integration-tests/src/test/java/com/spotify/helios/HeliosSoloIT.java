package com.spotify.helios;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.testing.Prober;
import com.spotify.helios.testing.TemporaryJob;
import com.spotify.helios.testing.TemporaryJobBuilder;
import com.spotify.helios.testing.TemporaryJobs;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.net.Socket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.helios.Utils.soloImage;
import static com.spotify.helios.system.SystemTestBase.ALPINE;
import static com.spotify.helios.system.SystemTestBase.BUSYBOX;
import static com.spotify.helios.system.SystemTestBase.IDLE_COMMAND;
import static com.spotify.helios.system.SystemTestBase.NGINX;
import static java.lang.System.getenv;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;

public class HeliosSoloIT {

  @Rule
  public final TemporaryJobs temporaryJobs = TemporaryJobs.create();

  private static final String TEST_USER = "HeliosIT";
  private static final String TEST_HOST = "solo.local";

  private static HeliosClient soloClient;

  @Before
  public void setup() throws Exception {
    // we're going to start helios-solo in a helios job. figure out the docker host, cert path, and
    // other stuff that helios-solo needs.
    String dockerHost = Optional.fromNullable(getenv("DOCKER_HOST"))
        .or("unix:///var/run/docker.sock");
    String certPath = getenv("DOCKER_CERT_PATH");

    final DockerClient docker = DefaultDockerClient.fromEnv().build();
    if (docker.info().kernelVersion().contains("tinycore64")) {
      // using boot2docker, so use the unix socket endpoint
      dockerHost = "unix:///var/run/docker.sock";
      certPath = null;
    }

    // use a probe container to get the correct value for HELIOS_HOST_ADDRESS
    final TemporaryJob probe = temporaryJobs.job()
        .image(BUSYBOX)
        .command(IDLE_COMMAND)
        .deploy();

    final String hostAddress = temporaryJobs.client()
        .hostStatus(probe.hosts().get(0)).get()
        .getEnvironment()
        .get("HELIOS_HOST_ADDRESS");

    // build the helios-solo job
    final TemporaryJobBuilder solo = temporaryJobs.job()
        .image(soloImage())
        .prober(new SoloStatusProber())
        .port("helios", 5801, 55801)
        .env("HELIOS_ID", "solo_it")
        .env("HELIOS_NAME", TEST_HOST)
        .env("DOCKER_HOST", dockerHost)
        .env("REGISTRAR_HOST_FORMAT", "_${service}._${protocol}.services.${domain}");

    if (!isNullOrEmpty(hostAddress)) {
      solo.env("HOST_ADDRESS", hostAddress);
    }

    if (!isNullOrEmpty(certPath)) {
      solo.env("DOCKER_CERT_PATH", "/certs")
          .volume("/certs", certPath);
    }

    if (dockerHost.startsWith("unix:///")) {
      solo.volume("/var/run/docker.sock", dockerHost.replace("unix://", ""));
    }

    // deploy the helios-solo job and create a Helios client for talking to it
    final String masterEndpoint = "http://" + solo.deploy().address("helios").toString();
    soloClient = HeliosClient.newBuilder()
        .setEndpoints(masterEndpoint)
        .setUser(TEST_USER)
        .build();
  }

  @Test
  public void soloTest() throws Exception {
    // run some jobs on the helios-solo cluster that we just brought up (inception/mind blown)
    assertThat(testResult(HeliosSoloITImpl.class), isSuccessful());
    assertTrue("jobs are running that should not be",
               soloClient.jobs().get(15, SECONDS).isEmpty());
  }

  public static class HeliosSoloITImpl {

    private TemporaryJob alpine;

    @Rule
    public final TemporaryJobs soloTemporaryJobs = TemporaryJobs.builder()
        .hostFilter(TEST_HOST)
        .client(soloClient)
        .prefixDirectory("/tmp/helios-solo-jobs")
        .build();


    @Before
    public void setup() throws Exception {
      // start a container that runs nginx and registers with SkyDNS
      soloTemporaryJobs.job()
          .image(NGINX)
          .port("http", 80, 59980)
          .registration("nginx", "http", "http")
          .deploy();

      // run a container that does SRV lookup to find the nginx service and then curl's it
      alpine = soloTemporaryJobs.job()
          .image(ALPINE)
          .port("nc", 4711, 54711)
          .command("sh", "-c",
                   "apk-install bind-tools " +
                   "&& export SRV=$(dig -t SRV +short _nginx._http.services.$SPOTIFY_DOMAIN) " +
                   "&& export HOST=$(echo $SRV | cut -d' ' -f4) " +
                   "&& export PORT=$(echo $SRV | cut -d' ' -f3) " +
                   "&& nc -lk -p 4711 -e curl http://$HOST:$PORT"
          )
          .deploy();
    }

    @Test
    public void test() throws Exception {
      final HostAndPort alpineAddress = alpine.address("nc");

      // Connect to alpine container to get the curl response. If we get back the nginx welcome page
      // we know that helios properly registered the nginx service in SkyDNS.
      try (final Socket s = new Socket(alpineAddress.getHostText(), alpineAddress.getPort())) {
        final String result = IOUtils.toString(s.getInputStream()).trim();
        assertThat(result, containsString("Welcome to nginx!"));
      }
    }
  }

  private class SoloStatusProber implements Prober {
    @Override
    public boolean probe(String host, int port) {
      try {
        final HeliosClient soloClient = HeliosClient.newBuilder()
            .setEndpoints("http://" + host + ":" + port)
            .setUser(TEST_USER)
            .build();

        final HostStatus hostStatus = soloClient.hostStatus(TEST_HOST).get(30, TimeUnit.SECONDS);
        return hostStatus != null && hostStatus.getStatus() == HostStatus.Status.UP;
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw Throwables.propagate(e);
      }
    }
  }

}