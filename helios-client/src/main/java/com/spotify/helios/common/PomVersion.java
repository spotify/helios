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

package com.spotify.helios.common;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.size;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Splitter;
import com.google.common.collect.ComparisonChain;
import org.jetbrains.annotations.NotNull;

public class PomVersion implements Comparable<PomVersion> {
  private final boolean isSnapshot;
  private final int major;
  private final int minor;
  private final int patch;

  public PomVersion(@JsonProperty("snapshot") boolean isSnapshot,
                    @JsonProperty("major") int major,
                    @JsonProperty("minor") int minor,
                    @JsonProperty("patch") int patch) {
    this.isSnapshot = isSnapshot;
    this.major = major;
    this.minor = minor;
    this.patch = patch;
  }

  public boolean isSnapshot() {
    return isSnapshot;
  }

  public int getMajor() {
    return major;
  }

  public int getMinor() {
    return minor;
  }

  public int getPatch() {
    return patch;
  }

  @Override
  public String toString() {
    return String.format("%d.%d.%d%s", major, minor, patch, isSnapshot ? "-SNAPSHOT" : "");
  }

  public static PomVersion parse(final String str) {
    boolean isSnapshot = false;
    String version = str;
    if (str.endsWith("-SNAPSHOT")) {
      isSnapshot = true;
      version = version.substring(0, str.length() - 9);
    }

    final Iterable<String> bits = Splitter.on(".").split(version);
    if (size(bits) != 3) {
      throw new RuntimeException("Version string format is invalid");
    }
    try {
      final Integer newMajor = Integer.valueOf(get(bits, 0));
      final Integer newMinor = Integer.valueOf(get(bits, 1));
      final Integer newPatch = Integer.valueOf(get(bits, 2));
      return new PomVersion(isSnapshot, newMajor, newMinor, newPatch);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Version portions are not numbers! " + str, e);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (isSnapshot ? 1231 : 1237);
    result = prime * result + major;
    result = prime * result + minor;
    result = prime * result + patch;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final PomVersion other = (PomVersion) obj;
    if (isSnapshot != other.isSnapshot) {
      return false;
    }
    if (major != other.major) {
      return false;
    }
    if (minor != other.minor) {
      return false;
    }
    return patch == other.patch;
  }

  @Override
  public int compareTo(@NotNull PomVersion pv) {
    return ComparisonChain.start()
        .compare(this.major, pv.major)
        .compare(this.minor, pv.minor)
        .compare(this.patch, pv.patch)
        .compareTrueFirst(this.isSnapshot, pv.isSnapshot)
        .result();
  }
}
