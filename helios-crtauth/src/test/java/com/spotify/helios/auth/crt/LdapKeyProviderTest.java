/*
 * Copyright (c) 2014 Spotify AB.
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

import com.google.common.collect.ImmutableList;

import com.spotify.crtauth.exceptions.KeyNotFoundException;
import com.spotify.helios.auth.crt.LdapKeyProvider.KeyParsingFunction;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.ldap.core.AttributesMapper;
import org.springframework.ldap.core.LdapOperations;
import org.springframework.ldap.query.LdapQuery;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.RSAPublicKeySpec;
import java.util.List;
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LdapKeyProviderTest {

  private final LdapOperations ldapTemplate = mock(LdapOperations.class);

  private final KeyParsingFunction validKeyParser = new KeyParsingFunction() {
    @Override
    public RSAPublicKeySpec apply(final String input) throws InvalidKeyException {
      // actually generates a random key of sufficient length to satisfy the
      // java.security.KeyFactory requirements!
      Random rnd = new Random();
      BigInteger bi = BigInteger.probablePrime(512, rnd);
      return new RSAPublicKeySpec(bi, bi);
    }
  };

  private final KeyParsingFunction invalidKeyParser = new KeyParsingFunction() {
    @Override
    public RSAPublicKeySpec apply(final String input) throws InvalidKeyException {
      throw new InvalidKeyException();
    }
  };

  private LdapKeyProvider keyProvider(KeyParsingFunction fn) {
    return new LdapKeyProvider(ldapTemplate, "CN=users", "pubkey", fn);
  }

  @Test
  public void keyFound() throws Exception {
    final LdapKeyProvider provider = keyProvider(validKeyParser);

    final List<String> results = ImmutableList.of("pubkey: foobar");

    final ArgumentCaptor<LdapQuery> query = ArgumentCaptor.forClass(LdapQuery.class);

    when(ldapTemplate.search(query.capture(), any(AttributesMapper.class)))
        .thenReturn(results);

    final RSAPublicKey key = provider.getKey("user123");

    // actual value is not important since we are returning random keys
    assertNotNull(key);

    assertThat(query.getValue().base().toString(), is("CN=users"));
    assertThat(query.getValue().filter().encode(), is("(uid=user123)"));
  }

  @Test(expected = KeyNotFoundException.class)
  public void noLdapSearchResults() throws Exception {
    final LdapKeyProvider provider = keyProvider(validKeyParser);

    final List<String> results = ImmutableList.of();

    when(ldapTemplate.search(any(LdapQuery.class), any(AttributesMapper.class)))
        .thenReturn(results);

    provider.getKey("user123");
  }

  @Test(expected = KeyNotFoundException.class)
  public void storedKeyIsInvalid() throws Exception {
    final LdapKeyProvider provider = keyProvider(invalidKeyParser);

    final List<String> results = ImmutableList.of("pubkey: a bunch of binary");

    when(ldapTemplate.search(any(LdapQuery.class), any(AttributesMapper.class)))
        .thenReturn(results);

    provider.getKey("user123");
  }
}