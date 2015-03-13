package com.spotify.helios;

import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerCertificates;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.HostInfo;
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
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.helios.Utils.soloImage;
import static com.spotify.helios.system.SystemTestBase.ALPINE;
import static com.spotify.helios.system.SystemTestBase.BUSYBOX;
import static com.spotify.helios.system.SystemTestBase.IDLE_COMMAND;
import static com.spotify.helios.system.SystemTestBase.NGINX;
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
    // fun times. we're going to start helios-solo as a temporary job in helios.

    // use a probe container to inspect the helios agent environment. we need to inject the same
    // environment into the helios-solo job so that it can find Docker, etc.
    final TemporaryJob probe = temporaryJobs.job()
        .image(BUSYBOX)
        .command(IDLE_COMMAND)
        .deploy();
    final HostStatus hostStatus = temporaryJobs.client()
        .hostStatus(probe.hosts().get(0)).get();
    final Map<String, String> hostEnvironment = hostStatus.getEnvironment();
    final HostInfo hostInfo = hostStatus.getHostInfo();
    probe.undeploy();

    // get values from the agent environment
    final String hostAddress = fromNullable(hostEnvironment.get("HELIOS_HOST_ADDRESS"))
        .or(probe.hosts().get(0));
    String dockerHost = hostInfo.getDockerHost();
    String certPath = hostInfo.getDockerCertPath();

    boolean useUnixSocketEndpoint = false;
    if (isNullOrEmpty(dockerHost)) {
      // no docker host specified, so fall back to the unix socket endpoint.
      useUnixSocketEndpoint = true;
    } else if (!dockerHost.startsWith("unix:///")) {
      if (dockerHost.contains("127.0.0.1")) {
        // the docker host is localhost. this can't be reached from within the helios-solo container
        // so try to fallback to the unix socket endpoint instead.
        useUnixSocketEndpoint = true;
      } else {
        // check if the docker instance used by the agent is Boot2Docker. if so use the unix socket
        // endpoint to avoid having to deal with Boot2Docker TLS certificate messiness.
        final String dockerUri = dockerHost.replace("tcp://", (isNullOrEmpty(certPath) ?
                                                               "http://" : "https://"));
        final DefaultDockerClient.Builder docker = DefaultDockerClient.builder().uri(dockerUri);
        if (!isNullOrEmpty(certPath)) {
          docker.dockerCertificates(new DockerCertificates(Paths.get(certPath)));
        }
        if (docker.build().info().operatingSystem().contains("Boot2Docker")) {
          // using boot2docker, so use the unix socket endpoint
          useUnixSocketEndpoint = true;
        }
      }
    }

    if (useUnixSocketEndpoint) {
      dockerHost = DefaultDockerClient.DEFAULT_UNIX_ENDPOINT;
      certPath = null;
    }

    // build the helios-solo job
    final TemporaryJobBuilder solo = temporaryJobs.job()
        .image(soloImage())
        .prober(new SoloStatusProber())
        .port("helios", 5801, 55801)
        .env("HELIOS_ID", "solo_it")
        .env("HELIOS_NAME", TEST_HOST)
        .env("HOST_ADDRESS", hostAddress)
        .env("DOCKER_HOST", dockerHost)
        .env("REGISTRAR_HOST_FORMAT", "_${service}._${protocol}.services.${domain}");

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
    public final TemporaryJobs soloTemporaryJobs = TemporaryJobs.builder("local")
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