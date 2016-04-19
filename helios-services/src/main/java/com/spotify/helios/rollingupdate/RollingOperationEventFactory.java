/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.rollingupdate;

import com.google.common.collect.Maps;

import com.spotify.helios.common.descriptors.RollingOperation;
import com.spotify.helios.common.descriptors.RolloutTask;

import java.util.Collections;
import java.util.Map;

public class RollingOperationEventFactory {

  private Map<String, Object> createEvent(final String eventType,
                                          final RollingOperation rolling) {
    final Map<String, Object> ev = Maps.newHashMap();
    ev.put("eventType", eventType);
    ev.put("timestamp", System.currentTimeMillis());
    ev.put("rollingOperation", rolling);
    return ev;
  }

  private Map<String, Object> addTaskFields(final Map<String, Object> ev,
                                            final RolloutTask task) {
    ev.put("action", task.getAction());
    ev.put("target", task.getTarget());
    return ev;
  }

  public Map<String, Object> rollingUpdateTaskFailed(final RollingOperation rolling,
                                                     final RolloutTask task,
                                                     final String error,
                                                     final RollingOperationError errorCode) {
    return rollingUpdateTaskFailed(rolling, task, error, errorCode,
                                   Collections.<String, Object>emptyMap());
  }

  public Map<String, Object> rollingUpdateTaskFailed(final RollingOperation rolling,
                                                     final RolloutTask task,
                                                     final String error,
                                                     final RollingOperationError errorCode,
                                                     final Map<String, Object> metadata) {
    final Map<String, Object> ev = createEvent("rollingUpdateTaskResult", rolling);
    ev.putAll(metadata);
    ev.put("success", 0);
    ev.put("error", error);
    ev.put("errorCode", errorCode);
    return addTaskFields(ev, task);
  }

  public Map<String, Object> rollingUpdateTaskSucceeded(final RollingOperation rolling,
                                                        final RolloutTask task) {
    final Map<String, Object> ev = createEvent("rollingUpdateTaskResult", rolling);
    ev.put("success", 1);
    return addTaskFields(ev, task);
  }

  public Map<String, Object> rollingUpdateStarted(final RollingOperation rolling) {
    final Map<String, Object> ev = createEvent("rollingUpdateStarted", rolling);
    return ev;
  }

  public Map<String, Object> rollingUpdateDone(final RollingOperation rolling) {
    final Map<String, Object> ev = createEvent("rollingUpdateFinished", rolling);
    ev.put("success", 1);
    return ev;
  }

  public Map<String, Object> rollingUpdateFailed(final RollingOperation rolling,
                                                 final Map<String, Object> failEvent) {
    final Map<String, Object> ev = createEvent("rollingUpdateFinished", rolling);
    ev.put("success", 0);
    ev.put("failedTask", failEvent);
    return ev;
  }
}
