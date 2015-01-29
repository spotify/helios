package com.spotify.helios;

import com.google.common.base.Charsets;
import com.google.common.net.HostAndPort;

import com.spotify.helios.testing.TemporaryJob;
import com.spotify.helios.testing.TemporaryJobs;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.net.Socket;

import static java.lang.String.format;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

/**
 * This test verifies that TemporaryJobs works correctly with SkyDNS. SkyDNS must be running
 * for the test to pass. It's easiest to use the vagrant image in the helios root directory
 * which will install and run SkyDNS.
 */
@Ignore
public class TemporaryJobsSkyDnsITCase {

  @Rule
  public final TemporaryJobs temporaryJobs = TemporaryJobs.create();

  private TemporaryJob job;

  @Before
  public void setup() {
    // When this job gets deployed, helios will register it with SkyDNS, using the randomized
    // prefix string as part of the SRV record. The container will then use dig to lookup that
    // SRV record, and expose the response on port 4711 via netcat.
    job = temporaryJobs.job()
        .image("rculbertson/dnsutils_netcat-traditional")
        .command("bash", "-c", "while true;" +
                               "do nc -p 4711 -lc 'dig -t srv +short lookup.tcp." +
                               temporaryJobs.prefix() + ".skydns.local'; " +
                               "done")
        .port("lookup", 4711, false)
        .registration("lookup", "tcp", "lookup")
        .deploy();
  }

  @Test
  public void test() throws Exception {
    final HostAndPort hostAndPort = job.address("lookup");
    final String host = hostAndPort.getHostText();
    final int port = hostAndPort.getPort();

    // Connect to the container to get the dig response. If we get back the correct value, we know
    // that helios properly registered the service in SkyDNS, and other services will be able to
    // find that service by doing a DNS lookup.
    try (final Socket s = new Socket(host, port)) {
      final byte[] bytes = new byte[32];
      final int bytesRead = s.getInputStream().read(bytes);
      assertThat(bytesRead, greaterThan(0));
      final String result = new String(bytes, Charsets.UTF_8).trim();
      assertThat(result, equalTo(format("10 100 %d %s.", port, host)));
    }
  }

}
