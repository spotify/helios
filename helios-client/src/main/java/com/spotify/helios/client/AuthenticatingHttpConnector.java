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

package com.spotify.helios.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;

import com.spotify.helios.client.HttpsHandlers.AuthenticatingHttpsHandler;
import com.spotify.helios.common.HeliosException;
import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

/**
 * HttpConnector that wraps another connector to add the necessary hooks into HttpURLConnection for
 * our SSH/TLS-based authentication.
 */
public class AuthenticatingHttpConnector implements HttpConnector {

  private static final Logger log = LoggerFactory.getLogger(AuthenticatingHttpConnector.class);

  // TODO (mbrown): username is here to pass to SshAgentSSLSocketFactory, could be handled nicer
  private final String user;
  private final Optional<AgentProxy> agentProxy;
  private final List<Identity> identities;
  private final EndpointIterator endpointIterator;

  private final DefaultHttpConnector delegate;

  public AuthenticatingHttpConnector(final String user,
                                     final Optional<AgentProxy> agentProxyOpt,
                                     final EndpointIterator endpointIterator,
                                     final DefaultHttpConnector delegate) {
    this(user, agentProxyOpt, endpointIterator, delegate, getSshIdentities(agentProxyOpt));
  }

  @VisibleForTesting
  AuthenticatingHttpConnector(final String user,
                              final Optional<AgentProxy> agentProxyOpt,
                              final EndpointIterator endpointIterator,
                              final DefaultHttpConnector delegate,
                              final List<Identity> identities) {
    this.user = user;
    this.agentProxy = agentProxyOpt;
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

    final Deque<Identity> ids;
    if (ipUri.getScheme().equalsIgnoreCase("https")) {
      ids = Queues.newArrayDeque(identities);
    } else {
      ids = Queues.newArrayDeque();
    }

    try {
      while (true) {
        final Identity identity = ids.poll();

        try {
          log.debug("connecting to {}", ipUri);

          if (agentProxy.isPresent() && identity != null) {
            delegate.setExtraHttpsHandler(new AuthenticatingHttpsHandler(
                user, agentProxy.get(), identity));
          }
          final HttpURLConnection connection = delegate.connect(ipUri, method, entity, headers);

          final int responseCode = connection.getResponseCode();
          if (responseCode == HTTP_BAD_GATEWAY) {
            log.debug("502 Bad Gateway");
            continue;
          }

          if (((responseCode == HTTP_FORBIDDEN) || (responseCode == HTTP_UNAUTHORIZED))
              && !ids.isEmpty()) {
            // there was some sort of security error. if we have any more SSH identities to try,
            // retry with the next available identity
            log.debug("retrying with next SSH identity since {} failed",
                      identity == null ? "the previous one" : identity.getComment());
            continue;
          }

          return connection;
        } catch (ConnectException | SocketTimeoutException | UnknownHostException e) {
          // UnknownHostException happens if we can't resolve hostname into IP address.
          // UnknownHostException's getMessage method returns just the hostname which is a
          // useless message, so log the exception class name to provide more info.
          log.debug(e.toString());
          throw new HeliosException("Unable to connect to master", e);
        }
      }
    } catch (IOException e) {
      throw new HeliosException(e);
    }
  }

  private URI toIpUri(Endpoint endpoint, URI uri) throws URISyntaxException {
    final URI endpointUri = endpoint.getUri();
    final String fullpath = endpointUri.getPath() + uri.getPath();

    final String uriScheme = endpointUri.getScheme();

    return new URI(uriScheme,
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
        log.debug("Couldn't get identities from ssh-agent", e);
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
