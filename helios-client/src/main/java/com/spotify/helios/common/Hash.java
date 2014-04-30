/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static com.google.common.base.Throwables.propagate;

public class Hash {

  public static byte[] sha1digest(final byte[] bytes) {
    return sha1().digest(bytes);
  }

  public static MessageDigest sha1() {
    final MessageDigest SHA1;
    try {
      SHA1 = MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException e) {
      throw propagate(e);
    }
    return SHA1;
  }
}
