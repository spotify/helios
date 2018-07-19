/*-
 * -\-\-
 * Helios Client
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

/**
 * Supplies an Authorization header value to use in HTTP requests to the Helios API.
 * <p>
 * If an implementation has no header value to return, then an empty Optional
 * should be returned.</p>
 */
public interface AuthorizationHeaderSupplier extends Supplier<Optional<String>> {
}
