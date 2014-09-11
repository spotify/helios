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

package com.spotify.helios.common.context;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class ContextTest {
  private ExecutorService executorService;
  private final Callable<Boolean> KABOOM_CALLABLE = new Callable<Boolean>() {
    @Override
    public Boolean call() throws Exception {
      throw new RuntimeException("No boolean for you");
    }
  };

  @Before
  public void setUp() throws Exception {
    executorService = Executors.newCachedThreadPool();
  }

  @After
  public void tearDown() throws Exception {
    executorService.shutdownNow();
  }

  @Test
  public void testContextExecutorService() throws Exception {
    final ExecutorService svc = Context.decorate(executorService);
    try {
      svc.submit(KABOOM_CALLABLE).get();
    } catch (ExecutionException e) {
      final Throwable rte = e.getCause();
      final Throwable cptee = rte.getCause();
      assertEquals(CallPathToExecutorException.class, cptee.getClass());
      assertEquals("testContextExecutorService", cptee.getStackTrace()[2].getMethodName());
    }
  }

  @Test
  public void testContextCallable() throws Exception {
    try {
      executorService.submit(Context.makeContextCallable(KABOOM_CALLABLE)).get();
    } catch (ExecutionException e) {
      final Throwable rte = e.getCause();
      final Throwable cptee = rte.getCause();
      assertEquals(CallPathToExecutorException.class, cptee.getClass());
      assertEquals("testContextCallable", cptee.getStackTrace()[1].getMethodName());
    }
  }

  @Test
  public void testContextRunnable() throws Exception {
    try {
      executorService.submit(Context.makeContextRunnable(new Runnable() {
        @Override
        public void run() {
          throw new RuntimeException("Oy vey!");
        }
      })).get();
    } catch (ExecutionException e) {
      final Throwable rte = e.getCause(); //the RuntimeException above
      final Throwable cptee = rte.getCause(); // should be CallPathToExecutorException
      assertEquals(CallPathToExecutorException.class, cptee.getClass());
      assertEquals("testContextRunnable", cptee.getStackTrace()[1].getMethodName());
    }
  }
}
