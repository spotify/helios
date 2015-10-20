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

package com.spotify.helios.auth.crt;

import com.google.common.annotations.VisibleForTesting;

import com.spotify.crtauth.exceptions.KeyNotFoundException;
import com.spotify.crtauth.keyprovider.KeyProvider;
import com.spotify.crtauth.utils.TraditionalKeyParser;

import org.springframework.ldap.core.AttributesMapper;
import org.springframework.ldap.core.LdapOperations;

import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.util.List;

import javax.naming.NamingException;
import javax.naming.directory.Attributes;

import static org.springframework.ldap.query.LdapQueryBuilder.query;

/**
 * Returns user data from an LDAP directory.
 */
public class LdapKeyProvider implements KeyProvider {

  // similar to Guava's Function, but we need to declare a checked exception
  public interface KeyParsingFunction {

    RSAPublicKeySpec apply(String input) throws InvalidKeyException;
  }

  private static final KeyParsingFunction DEFAULT_KEY_PARSER = new KeyParsingFunction() {
    @Override
    public RSAPublicKeySpec apply(final String input) throws InvalidKeyException {
      return TraditionalKeyParser.parsePemPublicKey(input);
    }
  };

  private final LdapOperations ldapTemplate;
  private final String baseSearchPath;
  /** name of the ldap attribute holding the key */
  private final String fieldName;
  private final KeyParsingFunction publicKeyParser;

  public LdapKeyProvider(final LdapOperations ldapTemplate,
                         final String baseSearchPath,
                         final String fieldName) {
    this(ldapTemplate, baseSearchPath, fieldName, DEFAULT_KEY_PARSER);
  }

  /**
   * Allow for swapping out the logic around parsing the public key string as returned by LDAP into
   * an RSAPublicKeySpec, as having the test mock out actual legit public keys seems too annoying
   */
  @VisibleForTesting
  protected LdapKeyProvider(final LdapOperations ldapTemplate,
                            final String baseSearchPath,
                            final String fieldName,
                            final KeyParsingFunction publicKeyParser) {
    this.ldapTemplate = ldapTemplate;
    this.baseSearchPath = baseSearchPath;
    this.fieldName = fieldName;
    this.publicKeyParser = publicKeyParser;
  }

  @Override
  public RSAPublicKey getKey(final String username) throws KeyNotFoundException {
    final List<String> result = ldapTemplate.search(
        query()
            .base(baseSearchPath)
            .where("uid").is(username),
        new AttributesMapper<String>() {
          @Override
          public String mapFromAttributes(final Attributes attributes) throws NamingException {
            return attributes.get(fieldName.toLowerCase()).toString();
          }
        });

    if (result.isEmpty()) {
      throw new KeyNotFoundException();
    } else if (result.size() == 1) {
      final String r = result.get(0);
      try {
        final String sshPublicKey = r.replace(fieldName + ": ", "").trim();
        final RSAPublicKeySpec publicKeySpec = this.publicKeyParser.apply(sshPublicKey);
        final KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return (RSAPublicKey) keyFactory.generatePublic(publicKeySpec);
      } catch (InvalidKeyException | InvalidKeySpecException | NoSuchAlgorithmException e) {
        throw new KeyNotFoundException(e);
      }

    }

    throw new IllegalStateException("Found more than one LDAP user for name: " + username);
  }


}
