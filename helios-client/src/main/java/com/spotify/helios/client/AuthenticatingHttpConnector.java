/*-
 * -\-\-
 * Helios Client
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

package com.spotify.helios.client;

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.helios.common.HeliosException;
import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;
import com.spotify.sshagenttls.CertFileHttpsHandler;
import com.spotify.sshagenttls.CertKeyPaths;
import com.spotify.sshagenttls.SshAgentHttpsHandler;
import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import javax.security.auth.x500.X500Principal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpConnector that wraps another connector to add the necessary hooks into HttpURLConnection for
 * our SSH/TLS-based authentication.
 */
public class AuthenticatingHttpConnector implements HttpConnector {

  private static final Logger log = LoggerFactory.getLogger(AuthenticatingHttpConnector.class);

  private final String user;
  private final Optional<AgentProxy> agentProxy;
  private final Optional<CertKeyPaths> clientCertificatePath;
  private final List<Identity> identities;
  private final EndpointIterator endpointIterator;

  private final DefaultHttpConnector delegate;

  public AuthenticatingHttpConnector(final String user,
                                     final Optional<AgentProxy> agentProxyOpt,
                                     final Optional<CertKeyPaths> clientCertificatePath,
                                     final EndpointIterator endpointIterator,
                                     final DefaultHttpConnector delegate) {
    this(user, agentProxyOpt, clientCertificatePath, endpointIterator,
        delegate, getSshIdentities(agentProxyOpt));
  }

  @VisibleForTesting
  AuthenticatingHttpConnector(final String user,
                              final Optional<AgentProxy> agentProxyOpt,
                              final Optional<CertKeyPaths> clientCertificatePath,
                              final EndpointIterator endpointIterator,
                              final DefaultHttpConnector delegate,
                              final List<Identity> identities) {
    this.user = user;
    this.agentProxy = agentProxyOpt;
    this.clientCertificatePath = clientCertificatePath;
    this.endpointIterator = endpointIterator;
    this.delegate = delegate;
    this.identities = identities;
  }

  @Override
  public HttpURLConnection connect(final URI uri, final String method, final byte[] entity,
                                   final Map<String, List<String>> headers) throws HeliosException {
    final Endpoint endpoint = endpointIterator.next();

    // convert the URI whose hostname portion is a domain name into a URI where the host is an IP
    // as we expect there to be several different IP addresses besides a common domain name
    final URI ipUri;
    try {
      ipUri = toIpUri(endpoint, uri);
    } catch (URISyntaxException e) {
      throw new HeliosException(e);
    }

    try {
      log.debug("connecting to {}", ipUri);

      if (clientCertificatePath.isPresent()) {
        // prioritize using the certificate file if set
        return connectWithCertificateFile(ipUri, method, entity, headers);
      } else if (agentProxy.isPresent() && !identities.isEmpty()) {
        // ssh-agent based authentication
        return connectWithIdentities(identities, ipUri, method, entity, headers);
      } else {
        // no authentication
        return doConnect(ipUri, method, entity, headers);
      }

    } catch (ConnectException | SocketTimeoutException | UnknownHostException e) {
      // UnknownHostException happens if we can't resolve hostname into IP address.
      // UnknownHostException's getMessage method returns just the hostname which is a
      // useless message, so log the exception class name to provide more info.
      log.debug(e.toString());
      throw new HeliosException("Unable to connect to master: " + ipUri, e);
    } catch (IOException e) {
      throw new HeliosException("Unexpected error connecting to " + ipUri, e);
    }
  }

  private HttpURLConnection connectWithCertificateFile(final URI ipUri, final String method,
                                                       final byte[] entity,
                                                       final Map<String, List<String>> headers)
      throws HeliosException {

    final CertKeyPaths clientCertificatePath = this.clientCertificatePath.get();
    log.debug("configuring CertificateFileHttpsHandler with {}", clientCertificatePath);

    delegate.setExtraHttpsHandler(CertFileHttpsHandler.create(false, clientCertificatePath)
    );

    return doConnect(ipUri, method, entity, headers);
  }

  private HttpURLConnection connectWithIdentities(final List<Identity> identities, final URI uri,
                                                  final String method, final byte[] entity,
                                                  final Map<String, List<String>> headers)
      throws IOException, HeliosException {

    if (identities.isEmpty()) {
      throw new IllegalArgumentException("identities cannot be empty");
    }

    final Queue<Identity> queue = new LinkedList<>(identities);
    HttpURLConnection connection = null;
    while (!queue.isEmpty()) {
      final Identity identity = queue.poll();

      delegate.setExtraHttpsHandler(SshAgentHttpsHandler.builder()
          .setUser(user)
          .setFailOnCertError(false)
          .setAgentProxy(agentProxy.get())
          .setIdentity(identity)
          .setX500Principal(new X500Principal("C=US,O=Spotify,CN=helios-client"))
          .setCertCacheDir(Paths.get(System.getProperty("user.home"), ".helios"))
          .build());

      connection = doConnect(uri, method, entity, headers);

      // check the status and retry the request if necessary
      final int responseCode = connection.getResponseCode();

      final boolean retryResponse =
          responseCode == HTTP_FORBIDDEN || responseCode == HTTP_UNAUTHORIZED;

      if (retryResponse && !queue.isEmpty()) {
        // there was some sort of security error. if we have any more SSH identities to try,
        // retry with the next available identity
        log.debug("retrying with next SSH identity since {} failed",
            identity == null ? "the previous one" : identity.getComment());
        continue;
      }
      break;
    }
    return connection;
  }

  private HttpURLConnection doConnect(final URI uri, final String method, final byte[] entity,
                                      final Map<String, List<String>> headers)
      throws HeliosException {
    return delegate.connect(uri, method, entity, headers);
  }

  private URI toIpUri(Endpoint endpoint, URI uri) throws URISyntaxException {
    final URI endpointUri = endpoint.getUri();
    final String fullpath = endpointUri.getPath() + uri.getPath();
    return new URI(
        endpointUri.getScheme(),
        endpointUri.getUserInfo(),
        endpoint.getIp().getHostAddress(),
        endpointUri.getPort(),
        fullpath,
        uri.getQuery(),
        null);
  }

  private static List<Identity> getSshIdentities(final Optional<AgentProxy> agentProxyOpt) {
    // ssh identities (potentially) used in authentication
    final ImmutableList.Builder<Identity> listBuilder = ImmutableList.builder();
    if (agentProxyOpt.isPresent()) {
      try {
        final List<Identity> identities = agentProxyOpt.get().list();
        for (final Identity identity : identities) {
          if (identity.getPublicKey().getAlgorithm().equals("RSA")) {
            // only RSA keys will work with our TLS implementation
            listBuilder.add(identity);
          }
        }
      } catch (Exception e) {
        // We catch everything because right now the masters do not require authentication.
        // So delay reporting errors to the user until the servers return 401 Unauthorized.
        log.debug("Unable to get identities from ssh-agent. Note that this might not indicate"
                  + " an actual problem unless your Helios cluster requires authentication"
                  + " for all requests.", e);
      }
    }

    return listBuilder.build();
  }

  @Override
  public void close() throws IOException {
    if (agentProxy.isPresent()) {
      agentProxy.get().close();
    }
  }
}
