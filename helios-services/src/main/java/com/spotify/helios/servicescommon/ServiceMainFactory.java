package com.spotify.helios.servicescommon;

public interface ServiceMainFactory {

  ServiceMain newService(final String[] args) throws Exception;
}
