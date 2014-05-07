/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.testing;

interface Prober {

  boolean probe(String host, int port);
}
