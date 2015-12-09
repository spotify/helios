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

import com.spotify.helios.client.tls.SshAgentSSLSocketFactory;
import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import javax.net.ssl.HttpsURLConnection;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Provides various implementations of {@link HttpsHandler}.
 */
class HttpsHandlers {

  static class AuthenticatingHttpsHandler implements HttpsHandler {

    private final String user;
    private final AgentProxy agentProxy;
    private final Identity identity;

    AuthenticatingHttpsHandler(final String user,
                               final AgentProxy agentProxy,
                               final Identity identity) {
      if (isNullOrEmpty(user)) {
        throw new IllegalArgumentException();
      }

      this.user = user;
      this.agentProxy = checkNotNull(agentProxy);
      this.identity = checkNotNull(identity);
    }

    String getUser() {
      return user;
    }

    AgentProxy getAgentProxy() {
      return agentProxy;
    }

    Identity getIdentity() {
      return identity;
    }

    @Override
    public void handle(final HttpsURLConnection conn) {
      conn.setSSLSocketFactory(new SshAgentSSLSocketFactory(agentProxy, identity, user));
    }
  }

}
