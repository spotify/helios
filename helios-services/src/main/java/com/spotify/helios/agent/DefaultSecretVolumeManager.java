/*
 * Copyright (c) 2016 Spotify AB.
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

package com.spotify.helios.agent;


import com.spotify.helios.common.SecretVolumeException;
import com.spotify.helios.common.descriptors.SecretVolumeRequest;
import com.spotify.helios.common.descriptors.Secrets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;

import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;


public class DefaultSecretVolumeManager implements SecretVolumeManager {
  private static final Logger log = LoggerFactory.getLogger(DefaultSecretVolumeManager.class);

  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private static final String SCHEME = "http";

  private final OkHttpClient client;
  private final SmallFileReader reader;
  private final String baseUrl;
  private final String path;

  public DefaultSecretVolumeManager(final String baseUrl, final String path) {
    this(new OkHttpClient(), new FsSmallFileReader(), baseUrl, path);
  }

  public DefaultSecretVolumeManager(final OkHttpClient client,
                                    final SmallFileReader fileReader,
                                    final String baseUrl,
                                    final String path) {
    this.client = client;
    this.reader = fileReader;
    this.baseUrl = baseUrl;
    this.path = path;
  }

  public static String urlFromAddr(final InetSocketAddress addr) {
    return new HttpUrl.Builder()
        .scheme(SCHEME)
        .host(addr.getHostString())
        .port(addr.getPort())
        .build()
        .toString();
  }

  @Override
  public String create(final String id, final Secrets secretsCfg) throws SecretVolumeException {
    log.debug("Creating secret volume id {}", id);
    final SecretVolumeRequest svr = secretVolumeRequest(id, secretsCfg);

    final HttpUrl url = HttpUrl.parse(baseUrl)
        .newBuilder()
        .addEncodedPathSegment(id)
        .build();

    final Request request = new Request.Builder()
        .url(url)
        .post(RequestBody.create(JSON, svr.toJsonString()))
        .build();

    // TODO(negz): Is it okay to block on this call?
    final Call call = client.newCall(request);
    try (final Response response = call.execute()) {
      if (!response.isSuccessful()) {
        throw new SecretVolumeException(response.body().toString());
      }
    } catch (IOException e) {
      throw new SecretVolumeException("unable to create secret volume", e);
    }

    return Paths.get(path, id).toString();
  }

  private SecretVolumeRequest secretVolumeRequest(final String id, final Secrets config)
      throws SecretVolumeException {
    if (config.getKeyPairFromHost() == null) {
      throw new SecretVolumeException("must supply a path to a secret volume keypair");
    }

    try {
      final String certData = reader.readToString(config.getKeyPairFromHost().getCertificate());
      final String keyData = reader.readToString(config.getKeyPairFromHost().getPrivateKey());
      return SecretVolumeRequest.create(id, config.getSource(), certData, keyData);
    } catch (IOException e) {
      throw new SecretVolumeException("Unable to read keypair file", e);
    }
  }

  @Override
  public void destroy(final String id) throws SecretVolumeException {
    log.debug("Destroying secret volume id {}", id);
    final HttpUrl url = HttpUrl.parse(baseUrl)
        .newBuilder()
        .addEncodedPathSegment(id)
        .build();

    final Request request = new Request.Builder()
        .url(url)
        .delete()
        .build();

    // TODO(negz): Is it okay to block on this call?
    final Call call = client.newCall(request);
    try (final Response response = call.execute()) {
      if (!response.isSuccessful()) {
        throw new SecretVolumeException(response.body().toString());
      }
    } catch (IOException e) {
      throw new SecretVolumeException("unable to create secret volume", e);
    }
  }

}
