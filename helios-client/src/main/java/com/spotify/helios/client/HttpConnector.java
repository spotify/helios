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

import com.spotify.helios.common.HeliosException;
import java.io.Closeable;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;
import java.util.Map;

interface HttpConnector extends Closeable {

  // TODO (mbrown): it's ugly that this has the same list of parameters as RequestDispatcher
  // TODO (mbrown): should create a Request interface instead to hold all of them
  HttpURLConnection connect(final URI uri, final String method, final byte[] entity,
                            final Map<String, List<String>> headers) throws HeliosException;
}
