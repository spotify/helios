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

import com.google.common.base.Preconditions;

import java.nio.file.Path;

/** Holds the Paths to files necessary to construct a ClientCerticate. */
public class ClientCertificatePath {

  private final Path certificatePath;
  private final Path keyPath;

  public ClientCertificatePath(final Path certificatePath, final Path keyPath) {
    this.certificatePath = checkExists(certificatePath);
    this.keyPath = checkExists(keyPath);
  }

  private static Path checkExists(Path path) {
    Preconditions.checkNotNull(path);
    Preconditions.checkArgument(path.toFile().canRead(),
        path + " does not exist or cannot be read");
    return path;
  }

  public Path getCertificatePath() {
    return certificatePath;
  }

  public Path getKeyPath() {
    return keyPath;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ClientCertificatePath that = (ClientCertificatePath) o;

    if (!certificatePath.equals(that.certificatePath)) {
      return false;
    }
    return keyPath.equals(that.keyPath);

  }

  @Override
  public int hashCode() {
    int result = certificatePath.hashCode();
    result = 31 * result + keyPath.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "ClientCertificatePath{" +
           "certificatePath=" + certificatePath +
           ", keyPath=" + keyPath +
           '}';
  }
}
