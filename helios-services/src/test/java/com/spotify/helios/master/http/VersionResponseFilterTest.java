package com.spotify.helios.master.http;

import com.google.common.collect.Maps;

import com.spotify.helios.common.PomVersion;
import com.spotify.helios.common.Version;
import com.spotify.helios.common.VersionCompatibility;
import com.spotify.helios.system.SystemTestBase;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static com.spotify.helios.common.Version.POM_VERSION;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class VersionResponseFilterTest extends SystemTestBase {

  private PomVersion current;

  @Before
  public void setUp() throws Exception {
    startDefaultMaster();
    current = PomVersion.parse(Version.POM_VERSION);
  }

  @Test
  public void testEqual() throws Exception {
    final HttpURLConnection connection = doVersionRequest(Version.POM_VERSION);
    assertResponseCodeType(connection, 2);
    assertServerVersion(connection);
    assertVersionStatus(connection, "EQUAL");
  }

  private void assertResponseCodeType(final HttpURLConnection connection, final int hc)
      throws IOException {
    assertEquals(hc, connection.getResponseCode() / 100);
  }

  @Test
  public void testMajorVersion() throws Exception {
    final String newVersion = (current.getMajor() + 1) + ".0.0";
    final HttpURLConnection connection = doVersionRequest(newVersion);
    assertResponseCodeType(connection, 4);
    assertServerVersion(connection);
    assertVersionStatus(connection, "INCOMPATIBLE");
  }

  @Test
  public void testPatchVersion() throws Exception {
    final String newVersion = current.getMajor() + "." + current.getMinor() + "."
        + (current.getPatch() + 1);
    final HttpURLConnection connection = doVersionRequest(newVersion);
    assertResponseCodeType(connection, 2);
    assertServerVersion(connection);
    assertVersionStatus(connection, "COMPATIBLE");
  }

  @Test
  public void testMinorVersion() throws Exception {
    final String newVersion = current.getMajor() + "." + (current.getMinor() + 1) + ".0";
    final HttpURLConnection connection = doVersionRequest(newVersion);
    assertResponseCodeType(connection, 2);
    assertServerVersion(connection);
    assertVersionStatus(connection, "MAYBE");
  }

  @Test
  public void testMalformed() throws Exception {
    final HttpURLConnection connection = doVersionRequest("deadbeef");
    assertResponseCodeType(connection, 4);
    assertServerVersion(connection);
    assertVersionStatus(connection, "INVALID");
  }

  @Test
  public void testMissing() throws Exception {
    final HttpURLConnection connection = doVersionRequest(null);
    assertResponseCodeType(connection, 2);
    assertServerVersion(connection);
    assertVersionStatus(connection, "MISSING");
  }

  private void assertVersionStatus(final HttpURLConnection connection, final String status) {
    assertEquals(status, connection.getHeaderField(VersionCompatibility.HELIOS_VERSION_STATUS_HEADER));
  }

  private void assertServerVersion(final HttpURLConnection connection) {
    assertEquals(POM_VERSION, connection.getHeaderField(VersionCompatibility.HELIOS_SERVER_VERSION_HEADER));
  }

  private HttpURLConnection doVersionRequest(String version) throws URISyntaxException, IOException {
    final Map<String, List<String>> headers = Maps.newHashMap();
    headers.put("Content-Type", asList("application/json"));
    headers.put("Charset", asList("utf-8"));
    if (version != null) {
      headers.put("X-Helios-Version", asList(version));
    }
    final URI uri = new URI(masterEndpoint + "/jobs");
    final HttpURLConnection connection = connect(uri,  headers);
    return connection;
  }

  private HttpURLConnection connect(final URI uri, final Map<String, List<String>> headers)
      throws IOException {
    final HttpURLConnection connection;
    connection = (HttpURLConnection) uri.toURL().openConnection();
    connection.setInstanceFollowRedirects(false);
    for (Map.Entry<String, List<String>> header : headers.entrySet()) {
      for (final String value : header.getValue()) {
        connection.addRequestProperty(header.getKey(), value);
      }
    }
    connection.setRequestMethod("GET");
    connection.getResponseCode();
    return connection;
  }
}
