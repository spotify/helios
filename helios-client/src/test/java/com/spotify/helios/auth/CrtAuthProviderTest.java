/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.helios.auth;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.spotify.crtauth.CrtAuthClient;
import com.spotify.crtauth.CrtAuthServer;
import com.spotify.crtauth.agentsigner.AgentSigner;
import com.spotify.crtauth.exceptions.KeyNotFoundException;
import com.spotify.crtauth.keyprovider.InMemoryKeyProvider;
import com.spotify.crtauth.signer.SingleKeySigner;
import com.spotify.crtauth.utils.TraditionalKeyParser;
import com.spotify.helios.client.HeliosRequest;
import com.spotify.helios.client.RequestDispatcher;
import com.spotify.helios.client.Response;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatcher;

import java.net.URI;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.RSAPrivateKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CrtAuthProviderTest {

  private static final String PUBLIC_KEY =
      "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDK0wNhgGlFZfBoRBS+M8wGoyOOVunYYjeaoRXKFKfhx288ZIo87" +
      "WMfN6i5KnUTH3A/mYlVnK4bhchS6dUFisaXcURvFgY46pUSGuLTZxTe9anIIR/iT+V+8MRDHXffRGOCLEQUl0le" +
      "YTht0dc7rxaW42d83yC7uuCISbgWqOANvMkZYqZjaejOOGVpkApxLGG8K8RvNBBM8TYqE3DQHSyRVU6S9HWLbWF" +
      "+i8W2h4CLX2Quodf0c1dcqlftClHjdIyed/zQKhAo+FDcJrN+2ZDJ0mkYLVlJDZuLk/K/vSOwD3wXhby3cdHCsx" +
      "nRfy2Ylnt31VF0aVtlhW4IJ+5mMzmz davidxia@example.com";
  private static final String PRIVATE_KEY =
      "-----BEGIN RSA PRIVATE KEY-----\n" +
      "MIIEogIBAAKCAQEAytMDYYBpRWXwaEQUvjPMBqMjjlbp2GI3mqEVyhSn4cdvPGSK\n" +
      "PO1jHzeouSp1Ex9wP5mJVZyuG4XIUunVBYrGl3FEbxYGOOqVEhri02cU3vWpyCEf\n" +
      "4k/lfvDEQx1330RjgixEFJdJXmE4bdHXO68WluNnfN8gu7rgiEm4FqjgDbzJGWKm\n" +
      "Y2nozjhlaZAKcSxhvCvEbzQQTPE2KhNw0B0skVVOkvR1i21hfovFtoeAi19kLqHX\n" +
      "9HNXXKpX7QpR43SMnnf80CoQKPhQ3CazftmQydJpGC1ZSQ2bi5Pyv70jsA98F4W8\n" +
      "t3HRwrMZ0X8tmJZ7d9VRdGlbZYVuCCfuZjM5swIDAQABAoIBADtnoHbfQHYGDGrN\n" +
      "ffHTg+9xuslG5YjuA3EzuwkMEbvMSOU8YUzFDqInEDDjoZSvQZYvJw0/LbN79Jds\n" +
      "S2srIU1b7HpIzhu/gVfjLgpTB8bh1w95vDfxxLrwU9uAdwqaojaPNoV9ZgzRltB7\n" +
      "hHnDp28cPcRSKekyK+9fAB8K6Uy8N00hojBDwtwXM8C4PpQKod38Vd0Adp9dEdX6\n" +
      "Ro9suYb+d+qFalYbKIbjKWkll+ZiiGJjF1HSQCTwlzS2haPXUlbk57HnN+8ar+a3\n" +
      "ITTc2gbNuTqBRD1V/gCaD9F0npVI3mQ34eUADNVVGS0xw0pN4j++Da8KXP+pyn/G\n" +
      "DU/n8SECgYEA/KN4BTrg/LB7cGrzkMQmW26NA++htjiWHK3WTsQBKBDFyReJBn67\n" +
      "o9kMTHBP35352RfuJ3xEEJ0/ddqGEY/SzNk3HMTlxBbR5Xq8ye102dxfEO3eijJ/\n" +
      "F4VRSf9sFgdRoLvE62qLudytK4Ku9nnKoIqrMxFweTpwxzf2jjIKDbECgYEAzYXe\n" +
      "QxT1A/bfs5Qd6xoCVOAb4T/ALqFo95iJu4EtFt7nvt7avqL+Vsdxu5uBkTeEUHzh\n" +
      "1q47LFoFdGm+MesIIiPSSrbfZJ6ht9kw8EbF8Py85X4LBXey67JlzzUq+ewFEP91\n" +
      "do7uGQAY+BRwXtzzPqaVBVa94YOxdq/AGutrIqMCgYBr+cnQImwKU7tOPse+tbbX\n" +
      "GRa3+fEZmnG97CZOH8OGxjRiT+bGmd/ElX2GJfJdVn10ZZ/pzFii6TI4Qp9OXjPw\n" +
      "TV4as6Sn/EDVXXHWs+BfRKp059VXJ2HeQaKOh9ZAS/x9QANXwn/ZfhGdKQtyWHdb\n" +
      "yiiFeQyjI3EUFD0SZRya4QKBgA1QvQOvmeg12Gx0DjQrLTd+hY/kZ3kd8AUKlvHU\n" +
      "/qzaqD0PhzCOstfAeDflbVGRPTtRu/gCtca71lqidzYYuiAsHfXFP1fvhx64LZmD\n" +
      "nFNurHZZ4jDqfmcS2dHA6hXjGrjtNBkITZjFDtkTyev7eK74b/M2mXrA44CDBnk4\n" +
      "A2rtAoGAMv92fqI+B5taxlZhTLAIaGVFbzoASHTRl3eQJbc4zc38U3Zbiy4deMEH\n" +
      "3QTXq7nxWpE4YwHbgXAeJUGfUpE+nEZGMolj1Q0ueKuSstQg5p1nwhQIxej8EJW+\n" +
      "7siqmOTZDKzieik7KVzaJ/U02Q186smezKIuAOYtT8VCf9UksJ4=\n" +
      "-----END RSA PRIVATE KEY-----";
  private static final String SERVER_NAME = "example.com";
  private static final String USERNAME = "foo";
  private static final String AUTH_URI = "https://helios/_auth";

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private final RequestDispatcher dispatcher = mock(RequestDispatcher.class);
  private CrtAuthServer authServer;
  private CrtAuthProvider authProvider;
  private String challenge;
  private String authResponse;

  @Before
  public void setup() throws Exception {
    final InMemoryKeyProvider keyProvider = new InMemoryKeyProvider();

    try {
      final RSAPublicKeySpec publicKeySpec = TraditionalKeyParser.parsePemPublicKey(PUBLIC_KEY);
      final KeyFactory keyFactory = KeyFactory.getInstance("RSA");

      final RSAPublicKey publicKey = (RSAPublicKey) keyFactory.generatePublic(publicKeySpec);
      keyProvider.putKey(USERNAME, publicKey);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    authServer = new CrtAuthServer.Builder()
        .setServerName(SERVER_NAME)
        .setKeyProvider(keyProvider)
        .setSecret(new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef})
        .build();

    final SingleKeySigner signer = new SingleKeySigner(parseRSAPrivateKey(PRIVATE_KEY));
    authProvider = new CrtAuthProvider(dispatcher, SERVER_NAME, USERNAME, signer);
    challenge = authServer.createChallenge(CrtAuthClient.createRequest(USERNAME));
    final CrtAuthClient crtClient = new CrtAuthClient(signer, SERVER_NAME);
    authResponse = crtClient.createResponse(challenge);
  }

  @Test
  public void testSuccessfulAuth() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null, authHeaderOf("challenge:" + challenge))));

    when(dispatcher.request(authRequestOfType("response:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null,
                     authHeaderOf("token:" + authServer.createToken(authResponse)))));

    // Just check the token starts with "chap:".
    // The token changes every time because it's time-based.
    assertThat(authProvider.currentAuthorizationHeader(), Matchers.containsString("chap:"));
    // But renewing the header should return the same one.
    assertEquals(authProvider.currentAuthorizationHeader(),
                 authProvider.renewAuthorizationHeader(null).get());
  }

  @Test
  public void testBadChallengeStatus() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 404, null,
                     Collections.<String, List<String>>emptyMap())));

    exception.expect(RuntimeException.class);
    exception.expectMessage(containsString("Got a 404 status code during the crtauth handshake."));
    authProvider.currentAuthorizationHeader();
  }

  @Test
  public void testMissingChallengeHeader() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null,
                     Collections.<String, List<String>>emptyMap())));

    when(dispatcher.request(authRequestOfType("response:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null,
                     authHeaderOf("token:" + authServer.createToken(authResponse)))));

    exception.expect(RuntimeException.class);
    exception.expectMessage(containsString(
        "Didn't get an HTTP \"X-CHAP\" header X-CHAP during the crtauth handshake."));
    authProvider.currentAuthorizationHeader();
  }

  @Test
  public void testMalformedChallengeHeader() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null, authHeaderOf("brogram:" + challenge))));

    when(dispatcher.request(authRequestOfType("response:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null,
                     authHeaderOf("token:" + authServer.createToken(authResponse)))));

    exception.expect(RuntimeException.class);
    exception.expectMessage(containsString("Got an invalid HTTP X-CHAP header"));
    authProvider.currentAuthorizationHeader();
  }

  @Test
  public void testBadTokenStatus() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null, authHeaderOf("challenge:" + challenge))));

    when(dispatcher.request(authRequestOfType("response:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 404, null,
                     authHeaderOf("token:" + authServer.createToken(authResponse)))));

    exception.expect(RuntimeException.class);
    exception.expectMessage(containsString("Got a 404 status code during the crtauth handshake."));
    authProvider.currentAuthorizationHeader();
  }

  @Test
  public void testMissingTokenHeader() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null, authHeaderOf("challenge:" + challenge))));

    when(dispatcher.request(authRequestOfType("response:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null,
                     Collections.<String, List<String>>emptyMap())));

    exception.expect(RuntimeException.class);
    exception.expectMessage(containsString(
        "Didn't get an HTTP \"X-CHAP\" header X-CHAP during the crtauth handshake."));
    authProvider.currentAuthorizationHeader();
  }

  @Test
  public void testMalformedTokenHeader() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null, authHeaderOf("challenge:" + challenge))));

    when(dispatcher.request(authRequestOfType("response:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null,
                     authHeaderOf("token" + authServer.createToken(authResponse)))));

    exception.expect(RuntimeException.class);
    exception.expectMessage(containsString("Got an invalid HTTP X-CHAP header"));
    authProvider.currentAuthorizationHeader();
  }

  // TODO (dxia) Should we keep this test? It's testing AgentSigner more than CrtAuthProvider.
  @Test
  @Ignore
  public void testAgentSignerWithNoPrivateKey() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null, authHeaderOf("challenge:" + challenge))));

    final CrtAuthClient crtClient = new CrtAuthClient(new AgentSigner(), SERVER_NAME);

    exception.expect(KeyNotFoundException.class);
    crtClient.createResponse(challenge);
  }

  private static HeliosRequest authRequestOfType(final String name) {
    return argThat(new ArgumentMatcher<HeliosRequest>() {
      @Override
      public boolean matches(Object argument) {
        if (argument instanceof HeliosRequest) {
          final HeliosRequest req = (HeliosRequest) argument;
          final String authHeader = req.header("X-CHAP");
          if (!isNullOrEmpty(authHeader) && authHeader.startsWith(name)) {
            return true;
          }
        }
        return false;
      }
    });
  }

  private static PrivateKey parseRSAPrivateKey(final String privateKey) {
    try {
      final RSAPrivateKeySpec privateKeySpec = TraditionalKeyParser.parsePemPrivateKey(privateKey);
      final KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      return keyFactory.generatePrivate(privateKeySpec);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, List<String>> authHeaderOf(final String value) {
    return ImmutableMap.<String, List<String>>of("X-CHAP", ImmutableList.of(value));
  }
}
