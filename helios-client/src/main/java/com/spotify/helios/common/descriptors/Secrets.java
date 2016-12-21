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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.jetbrains.annotations.Nullable;

import java.security.KeyPair;
import java.util.Objects;

/**
 * Configures automatic secrets procurement for a container.
 *
 * <pre>
 *   {
 *     "source": "Talos",
 *     "path": "/secrets",
 *     "keypairFromHost": {
 *       "certificate": "/path/to/pem/cert/on/host",
 *       "privateKey": "/path/to/pem/key/on/host"
 *     }
 *   }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Secrets extends Descriptor {
  private final Source source;
  private final String path;
  private final KeyPair keyPairFromHost;

  /**
   * Automatic secret procurement configuration.
   *
   * @param source The source from which to procure secrets for this job.
   * @param path The path inside the container at which to mount the secrets for this job.
   * @param keyPairFromHost Optional keypair to use to authenticate to the secret source on behalf
   *                        of this container.
   */
  public Secrets(@JsonProperty("source") final Source source,
                 @JsonProperty("path") final String path,
                 @JsonProperty("keyPairFromHost") @Nullable final KeyPair keyPairFromHost) {
    // TODO(negz): Support tags. Inherit from agent labels, but allow override?
    this.source = source;
    this.path = path;
    this.keyPairFromHost = keyPairFromHost;
  }

  /**
   * Create a new secret procurement configuration.
   *
   * @param source The secret source (i.e. Talos)
   * @param path The path inside the container at which to mount the secrets for this job.
   * @param certPath The path on the host at which to find a certificate for secret authentication.
   * @param keyPath The path on the host at which to find a private key for secret authentication.
   * @return A new secret procurement configuration.
   */
  public static Secrets create(final Source source, final String path,
                               final String certPath, final String keyPath) {
    return new Secrets(source, path, new KeyPair(certPath, keyPath));
  }

  public Source getSource() {
    return this.source;
  }

  public String getPath() {
    return this.path;
  }

  @Nullable
  public KeyPair getKeyPairFromHost() {
    return this.keyPairFromHost;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Secrets that = (Secrets) o;

    return Objects.equals(this.source, that.source)
           && Objects.equals(this.path, that.path)
           && Objects.equals(this.keyPairFromHost, that.keyPairFromHost);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, path, keyPairFromHost);
  }

  @Override
  public String toString() {
    return "Secrets{"
           + "source=" + source
           + ", path=" + path
           + ", keyPairFromHost=" + keyPairFromHost
           + "} " + super.toString();
  }

  public enum Source {
    UNKNOWN,
    TALOS
  }

  public static class KeyPair {
    private final String certificate;
    private final String privateKey;

    /**
     * An SSL keypair for authenticating to a secret source.
     *
     * @param certificate A path to a PEM encoded certificate file.
     * @param privateKey A path to a PEM encoded private key.
     */
    public KeyPair(@JsonProperty("certificate") final String certificate,
                   @JsonProperty("privateKey") final String privateKey) {
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
