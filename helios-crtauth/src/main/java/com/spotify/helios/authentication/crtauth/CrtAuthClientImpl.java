/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package com.spotify.helios.authentication.crtauth;


import com.google.common.io.CharStreams;

import com.spotify.crtauth.CrtAuthClient;
import com.spotify.crtauth.agentsigner.AgentSigner;
import com.spotify.crtauth.exceptions.CrtAuthException;
import com.spotify.crtauth.exceptions.ProtocolVersionException;
import com.spotify.crtauth.signer.Signer;
import com.spotify.crtauth.signer.SingleKeySigner;
import com.spotify.crtauth.utils.TraditionalKeyParser;
import com.spotify.helios.authentication.AuthClient;
import com.spotify.helios.authentication.HeliosAuthException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPrivateKeySpec;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import static com.spotify.helios.authentication.crtauth.CrtAuthConfig.HEADER;

class CrtAuthClientImpl implements AuthClient {

  private static final Logger log = LoggerFactory.getLogger(CrtInjectableProvider.class);

  private final Path privateKeyPath;
  private final List<URI> authServerUris;
  private final Client client;
  private String token;

  public CrtAuthClientImpl(final Path privateKeyPath, final List<URI> authServerUris) {
    this.privateKeyPath = privateKeyPath;
    this.authServerUris = authServerUris;
    this.client = ClientBuilder.newClient();
  }

  public String getToken(final String username) throws HeliosAuthException {
    if (token == null) {
      // TODO (dxia) round-robin here
      final URI authServerUri = authServerUris.get(0);
      log.info("Sending authentication requests for user {} to server {}", username, authServerUri);
      // TODO (dxia) Check authentication scheme is CRT
//        final List<String> authHeaders = r.getHeaders().get(HttpHeaders.WWW_AUTHENTICATE);
//        if (authHeaders.size() < 1) {
//          log.info("something went wrong");
//        }
//        if (!authHeaders.get(0).equals(CRT_AUTH_SCHEME)) {
//          log.info("This client doesn't support auth scheme {}.", authHeaders.get(0));
//        }

      final String authRequest = CrtAuthClient.createRequest(username);
      final MultivaluedHashMap<String, Object> barheaders = new MultivaluedHashMap<>();
      barheaders.add(HEADER, "request:" + authRequest);
      final Response r2 = request(authServerUri, "/_auth", barheaders);

      if (r2.getStatus() != 200) {
        throw new HeliosAuthException(String.format(
            "CRT auth request failed with status code %d and message: %s",
            r2.getStatus(), r2.readEntity(String.class)));
      }

      MultivaluedMap<String, Object> headers = r2.getMetadata();
      final List<Object> xChapHeader = headers.get(HEADER);
      if (xChapHeader == null || xChapHeader.size() < 1) {
        throw new HeliosAuthException(String.format(
            "CRT auth request failed. No %s header in reply.", HEADER));
      }

      final String[] challengeParts = ((String) xChapHeader.get(0)).split(":");
      if (challengeParts.length != 2) {
        throw new HeliosAuthException(String.format(
            "CRT auth request failed to get valid challenge: %s.", xChapHeader.get(0)));
      }

      final String challenge = challengeParts[1];
      log.debug("Got CRT auth challenge {}", xChapHeader.get(0));

      final CrtAuthClient crtAuthClient;
      final String response;

      try {
        crtAuthClient = makeCrtAuthClient(privateKeyPath, authServerUri);
        response = crtAuthClient.createResponse(challenge);
        final MultivaluedHashMap<String, Object> fooheaders = new MultivaluedHashMap<>();
        fooheaders.add(HEADER, "response:" + response);
        final Response r3 = request(authServerUri, "/_auth", fooheaders);

        if (r3.getStatus() != 200) {
          throw new HeliosAuthException(String.format(
              "CRT auth response failed, status code %d and message: %s.",
              r3.getStatus(), r3.readEntity(String.class)));
        }

        final MultivaluedMap<String, Object> headers2 = r3.getMetadata();
        final List<Object> xChapHeader2 = headers2.get(HEADER);
        if (xChapHeader2 == null || xChapHeader2.size() < 1) {
          throw new HeliosAuthException(String.format(
              "CRT auth response failed to get %s header in reply.", HEADER));
        }

        final String[] tokenParts = ((String) xChapHeader2.get(0)).split(":");
        if (tokenParts.length != 2) {
          throw new HeliosAuthException(String.format(
              "CRT auth response failed to get valid token: %s.", xChapHeader2.get(0)));
        }

        final String token = tokenParts[1];
        log.debug("Got CRT auth token {}", xChapHeader2.get(0));
        this.token = "chap:" + token;
      } catch (ProtocolVersionException e) {
        throw new HeliosAuthException("Can't create valid signature for the CRT challenge. " +
                                      e.getMessage(), e);
      } catch (CrtAuthException e) {
        throw new HeliosAuthException("Unable to find private SSH key.");
      }
    }

    return token;
  }

  private static CrtAuthClient makeCrtAuthClient(final Path privateKeyPath,
                                                 final URI authServer)
      throws CrtAuthException {
    final Signer signer;

    // Try to parse private key file if not null
    if (privateKeyPath != null) {
      try {
        final String privateKeyStr = CharStreams
            .toString(new FileReader(privateKeyPath.toString()));
        final RSAPrivateKeySpec privateKeySpec =
            TraditionalKeyParser.parsePemPrivateKey(privateKeyStr);
        final KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        final PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);
        signer = new SingleKeySigner(privateKey);
      } catch (IOException | InvalidKeyException | NoSuchAlgorithmException |
          InvalidKeySpecException e) {
        throw new CrtAuthException(String.format(
            "Couldn't parse private key %s. If it's encrypted with a passphrase, make sure "
            + "ssh-agent is running, you've added your key to it, and don't specify your "
            + "private key in the helios command line. This helios-crtauth auth plugin will then "
            + "sign the CRT challenge by passing it to ssh-agent.", privateKeyPath.toString()), e);
      }
    } else {
      signer = new AgentSigner();
    }

    return new CrtAuthClient(signer, authServer.getHost());
  }

  private Response request(final URI authServerUri,
                           final String path,
                           final MultivaluedMap<String, Object> headers) {
    return client.target(authServerUri)
        .path(path)
        .request(MediaType.TEXT_PLAIN_TYPE)
        .headers(headers).get();
  }
}

