/*-
 * -\-\-
 * Helios Client
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

package com.spotify.helios.client;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.net.URI;
import java.util.List;
import java.util.Map;

interface RequestDispatcher extends Closeable {

  ListenableFuture<Response> request(
      URI uri, String method, byte[] entityBytes, Map<String, List<String>> headers);

}
