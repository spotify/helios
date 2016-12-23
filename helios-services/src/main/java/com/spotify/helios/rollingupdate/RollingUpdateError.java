/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.rollingupdate;

public enum RollingUpdateError {
  PORT_CONFLICT,
  JOB_UNEXPECTEDLY_UNDEPLOYED,
  JOB_ALREADY_DEPLOYED,
  TIMED_OUT_WAITING_FOR_JOB_TO_REACH_RUNNING,
  TIMED_OUT_WAITING_FOR_JOB_TO_UNDEPLOY,
  UNABLE_TO_MARK_HOST_UNDEPLOYED,
  TOKEN_VERIFICATION_ERROR,
  JOB_NOT_FOUND,
  HOST_NOT_FOUND,
  TIMED_OUT_RETRIEVING_JOB_STATUS,
  IMAGE_MISSING,
  IMAGE_PULL_FAILED,
  UNKNOWN
}
