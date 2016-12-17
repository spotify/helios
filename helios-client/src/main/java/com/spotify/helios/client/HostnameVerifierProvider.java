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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HostnameVerifierProvider {

  private static final Logger log = LoggerFactory.getLogger(HostnameVerifierProvider.class);

  private final boolean hostnameVerificationEnabled;
  private final HostnameVerifier delegate;

  public HostnameVerifierProvider(final boolean hostnameVerificationEnabled,
                                  final HostnameVerifier delegate) {
    this.hostnameVerificationEnabled = hostnameVerificationEnabled;
    this.delegate = delegate;
  }

  public HostnameVerifier verifierFor(String hostname) {
    // java 8 will make all these lines for the anon classes go poof

    if (!hostnameVerificationEnabled) {
      log.debug("hostname verification disabled");
      return new HostnameVerifier() {
        @Override
        public boolean verify(final String str, final SSLSession sslSession) {
          return true;
        }
      };
    }

    final String tHostname =
        hostname.endsWith(".") ? hostname.substring(0, hostname.length() - 1) : hostname;

    log.debug("configuring DefaultHostnameVerifier to expect hostname={}", tHostname);

    return new HostnameVerifier() {
      @Override
      public boolean verify(final String str, final SSLSession sslSession) {
        return delegate.verify(tHostname, sslSession);
      }
    };
  }
}
