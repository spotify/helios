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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import com.spotify.crtauth.exceptions.KeyNotFoundException;
import com.spotify.crtauth.keyprovider.KeyProvider;
import com.spotify.crtauth.utils.TraditionalKeyParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ldap.core.AttributesMapper;
import org.springframework.ldap.core.LdapTemplate;

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
// TODO (mbrown): generalize this
public class LdapKeyProvider implements KeyProvider {

  private static final Logger log = LoggerFactory.getLogger(LdapKeyProvider.class);

  private final LdapTemplate ldapTemplate;
  private final String baseSearchPath;
  /** name of the ldap attribute holding the key */
  private final String fieldName;

  public LdapKeyProvider(final LdapTemplate ldapTemplate,
                         final String baseSearchPath,
                         final String fieldName) {
    this.fieldName = fieldName;
    this.ldapTemplate = Preconditions.checkNotNull(ldapTemplate);
    this.baseSearchPath = Preconditions.checkNotNull(baseSearchPath);
  }

  @Override
  public RSAPublicKey getKey(final String username) throws KeyNotFoundException {
    final List<String> result = ldapTemplate.search(
        query()
            .base(baseSearchPath)
            .where("uid").is(username),
        new AttributesMapper<String>() {
          // TODO (mbrown): how spotify specific is this field.lowercase stuff?
          @Override
          public String mapFromAttributes(final Attributes attributes) throws NamingException {
            log.debug("got ldap stuff for uid {}", username);
            return attributes.get(fieldName.toLowerCase()).toString();
          }
        });

    if (result.isEmpty()) {
      throw new KeyNotFoundException();
    } else if (result.size() == 1) {
      final String r = result.get(0);
      RSAPublicKeySpec publicKeySpec;
      try {
        final String sshPublicKey = r.replace(fieldName + ": ", "").trim();
        publicKeySpec = TraditionalKeyParser.parsePemPublicKey(sshPublicKey);
        final KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return (RSAPublicKey) keyFactory.generatePublic(publicKeySpec);
      } catch (InvalidKeyException | InvalidKeySpecException | NoSuchAlgorithmException e) {
        throw Throwables.propagate(e);
      }

    }

    throw new IllegalStateException("Found more than one LDAP user for name: " + username);
  }
}
