/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios;

import com.spotify.helios.cli.LoggingConfig;
import com.spotify.helios.cli.Parser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MainTest {

  @Mock
  Parser parser;

  @Mock
  Entrypoint entrypoint;

  @Captor
  ArgumentCaptor<CountDownLatch> latchCaptor;

  Main main;

  @Before
  public void setup() throws Exception {
    when(parser.getEntrypoint()).thenReturn(entrypoint);
    when(parser.getLoggingConfig()).thenReturn(new LoggingConfig(
      0, false, null, false));

    main = new Main(parser);
  }

  @Test
  public void testMainRunsEntrypoint() throws Exception {
    // premise
    when(entrypoint.enter(latchCaptor.capture()))
        .thenReturn(0);

    // run
    int exit = main.run();
    verify(entrypoint).enter(latchCaptor.getValue());
    assertEquals(1, latchCaptor.getValue().getCount());
    assertEquals("main exited with code 0", 0, exit);

    // shutdown
    main.shutdown();
    assertEquals(0, latchCaptor.getValue().getCount());
  }

  @Test
  public void testMainHandlesEntrypointExceptions() throws Exception {
    // premise
    when(entrypoint.enter(latchCaptor.capture()))
        .thenThrow(RuntimeException.class);

    // run
    int exit = main.run();
    verify(entrypoint).enter(latchCaptor.getValue());
    assertEquals("main exited with code 1", 1, exit);
  }
}
