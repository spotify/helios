/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios.servicescommon.coordination;

import org.apache.zookeeper.KeeperException;

/**
 * A task that returns a result and may throw a {@link KeeperException}.
 * We use this class when we don't want a {@link java.util.concurrent.Callable}
 * that throws an Exception.
 */
@FunctionalInterface
public interface ZooKeeperCallable<T> {
  T call() throws KeeperException;
}
