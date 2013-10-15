/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.HeliosRun;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class ServiceEntrypointTest {

  @Mock HeliosRun heliosRun;

  final CountDownLatch latch = new CountDownLatch(1);

  ServiceEntrypoint sut;

  @Before
  public void setup() {
    sut = new ServiceEntrypoint(heliosRun);
  }

  @Test
  public void cleanRunReturnCode0() throws Exception {
    latch.countDown();
    int exit = sut.enter(latch);

    assertEquals("exited with code 0", 0, exit);
    verify(heliosRun).start();
    verify(heliosRun).stop();
    verifyNoMoreInteractions(heliosRun);
  }

  @Test
  public void runWithExceptionsReturnCode1() throws Exception {
    // premise
    doThrow(RuntimeException.class).when(heliosRun).start();

    // work
    latch.countDown();
    int exit = sut.enter(latch);

    // test
    assertEquals("exited with code 1", 1, exit);
    verify(heliosRun).start();
    verify(heliosRun).stop();
    verifyNoMoreInteractions(heliosRun);
  }
}
