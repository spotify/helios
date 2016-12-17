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

import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ClientCertificatePathTest {

  private Path tempFile;

  @Before
  public void setUp() throws Exception {
    tempFile = Files.createTempFile(getClass().getSimpleName(), "tmp");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCertificateDoesNotExist() {
    new ClientCertificatePath(Paths.get("some-unknown-file"), tempFile);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testKeyDoesNotExist() {
    new ClientCertificatePath(tempFile, Paths.get("some-unknown-file"));
  }

  @Test
  public void testEverythingExists() {
    new ClientCertificatePath(tempFile, tempFile);
  }
}
