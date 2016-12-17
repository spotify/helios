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

package com.spotify.helios.client.tls;

import com.google.common.base.Throwables;

import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Returns a signed hash of any arbitrary data by sending it to the local SSH agent.
 */
class SshAgentContentSigner implements ContentSigner {

  private static final AlgorithmIdentifier IDENTIFIER =
      new DefaultSignatureAlgorithmIdentifierFinder().find("SHA1withRSA");

  private final ByteArrayOutputStream stream = new ByteArrayOutputStream();

  private final AgentProxy agentProxy;
  private final Identity identity;

  public SshAgentContentSigner(final AgentProxy agentProxy, final Identity identity) {
    this.agentProxy = agentProxy;
    this.identity = identity;
  }

  @Override
  public AlgorithmIdentifier getAlgorithmIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public OutputStream getOutputStream() {
    return stream;
  }

  @Override
  public byte[] getSignature() {
    try {
      return agentProxy.sign(identity, stream.toByteArray());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
