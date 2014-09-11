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

import java.util.Arrays;

/**
 * Not indicative of an exception in and of itself, but it extends the stack trace
 * of Exceptions of *ables wrapped using Context.makeContext(Runnable|Callable).
 *
 * See Context.java for more details.
 */
public class CallPathToExecutorException extends Exception {
  private static final String CONTEXT_RUNNABLE = ContextRunnable.class.getCanonicalName();
  private static final String CONTEXT_CALLABLE = ContextCallable.class.getCanonicalName();

  CallPathToExecutorException(StackTraceElement[] trace) {
    super();
    int c = 0;
    for (StackTraceElement element : trace) {
      c++;
      // Trim the head exception so that it should start at makeContext*able
      final String className = element.getClassName();
      if (className.equals(CONTEXT_RUNNABLE)
          || className.equals(CONTEXT_CALLABLE)) {
        break;
      }
    }
    // Should never happen, but if it does, don't chop off everything.
    if (c == trace.length) {
      c = 0;
    }
    this.setStackTrace(Arrays.copyOfRange(trace, c, trace.length));
  }
}
