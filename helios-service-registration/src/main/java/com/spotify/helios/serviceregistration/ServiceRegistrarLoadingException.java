/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.serviceregistration;

import java.util.ServiceConfigurationError;

/**
 * Indicates that an error occurred during service registrar plugin loading.
 */
public class ServiceRegistrarLoadingException extends Exception {

  ServiceRegistrarLoadingException(final String message, final ServiceConfigurationError e) {
    super(message, e);
  }
}
