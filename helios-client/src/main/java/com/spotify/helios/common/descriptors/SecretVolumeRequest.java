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

package com.spotify.helios.common.descriptors;

import static java.util.Collections.emptyMap;

import com.spotify.helios.common.SecretVolumeException;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a secret volume request to secret-volume, i.e. a secret-volume response with an added
 * KeyPair.
 *
 * <pre>
 *   {
 *     "ID": "containerId",
 *     "Source": "Talos",
 *     "Tags": {},
 *     "KeyPair": {
 *        "Certificate": "PEM encoded cert",
 *        "PrivateKey": "Pem encoded private key"
 *     }
 *   }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SecretVolumeRequest extends SecretVolume {

  private final KeyPair keyPair;

  public SecretVolumeRequest(
      @JsonProperty("ID") final String id,
      @JsonProperty("Source") final Secrets.Source source,
      @JsonProperty("Tags") final Map<String, List<String>> tags,
      @JsonProperty("KeyPair") final KeyPair keyPair) {
    super(id, source, tags);
    this.keyPair = keyPair;
  }

  public static SecretVolumeRequest create(final String id, final Secrets.Source source,
                                           final String certData, final String keyData) {
    // TODO(negz): Read tags from config, or agent labels?
    final Map<String, List<String>> tags = emptyMap();
    return new SecretVolumeRequest(id, source, tags, new KeyPair(certData, keyData));
  }

  private static String readToString(final String path) throws SecretVolumeException {
    try {
      return new String(Files.readAllBytes(Paths.get(path)), Charset.defaultCharset());
    } catch (IOException e) {
      throw new SecretVolumeException("cannot read secret volume keypair", e);
    }
  }

  public KeyPair getKeyPair() {
    return this.keyPair;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final SecretVolumeRequest that = (SecretVolumeRequest) o;

    return Objects.equals(this.id, that.id)
           && Objects.equals(this.source, that.source)
           && Objects.equals(this.tags, that.tags)
           && Objects.equals(this.keyPair, that.keyPair);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, source, tags, keyPair);
  }

  @Override
  public String toString() {
    return "Secrets{"
           + "source=" + source
           + ", id=" + id
           + ", tags=" + tags
           + ", keyPair=" + keyPair
           + "} " + super.toString();
  }

  public static class KeyPair {
    private final String certificate;
    private final String privateKey;

    /**
     * An SSL keypair for authenticating to a secret source.
     *
     * Note that while this object is almost identical to Secrets.KeyPair it uses different JSON
     * keys, and is intended to store PEM encoded private keypair data, as opposed to the path to
     * the certificate and private key.
     *
     * @param certificate A PEM encoded certificate.
     * @param privateKey A PEM encoded private key.
     */
    public KeyPair(@JsonProperty("Certificate") final String certificate,
                   @JsonProperty("PrivateKey") final String privateKey) {
      this.certificate = certificate;
      this.privateKey = privateKey;
    }

    public String getCertificate() {
      return this.certificate;
    }

    public String getPrivateKey() {
      return this.privateKey;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final KeyPair that = (KeyPair) o;

      return Objects.equals(this.certificate, that.certificate)
             && Objects.equals(this.privateKey, that.privateKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(certificate, privateKey);
    }

    @Override
    public String toString() {
      return "KeyPair{"
             + "certificate=" + certificate
             + ", privateKey=" + privateKey
             + "} " + super.toString();
    }
  }
}
