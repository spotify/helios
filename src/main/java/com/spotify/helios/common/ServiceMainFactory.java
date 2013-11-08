package com.spotify.helios.common;

public interface ServiceMainFactory {
  ServiceMain newService(final String[] args) throws Exception;
}
