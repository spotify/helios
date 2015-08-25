/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.helios.rollingupdate;

public enum RollingUpdateError {
  PORT_CONFLICT,
  JOB_UNEXPECTEDLY_UNDEPLOYED,
  JOB_ALREADY_DEPLOYED,
  TIMED_OUT_WAITING_FOR_JOB_TO_REACH_RUNNING,
  TOKEN_VERIFICATION_ERROR,
  JOB_NOT_FOUND,
  HOST_NOT_FOUND,
  TIMED_OUT_RETRIEVING_JOB_STATUS
}
