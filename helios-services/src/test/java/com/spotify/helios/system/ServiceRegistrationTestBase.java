/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.serviceregistration.ServiceRegistration;
import com.spotify.helios.serviceregistration.ServiceRegistrationHandle;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class ServiceRegistrationTestBase extends SystemTestBase {

  private static final AtomicInteger registryIdCounter = new AtomicInteger();

  protected ServiceRegistrar mockServiceRegistrar() {
    final ServiceRegistrar registrar = mock(ServiceRegistrar.class);
    when(registrar.register(any(ServiceRegistration.class))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
        return mock(ServiceRegistrationHandle.class);
      }
    });
    return registrar;
  }

  protected String uniqueRegistryAddress() {
    return "mock://" + registryIdCounter.incrementAndGet();
  }
}
