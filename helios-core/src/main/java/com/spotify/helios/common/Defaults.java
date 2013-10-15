/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.common;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class Defaults {

  public static final int MASTER_HM_PORT = 5800;
  public static final int MASTER_HTTP_PORT = 5701;

  public static final String MASTER_HM_BIND = "tcp://*:" + MASTER_HM_PORT;
  public static final String MASTER_HTTP_BIND = "0.0.0.0:" + MASTER_HTTP_PORT;

  public static final List<String> SITES = ImmutableList.of("shared.cloud.spotify.net.");
  public static final String SRV_NAME = "helios-master";
}
