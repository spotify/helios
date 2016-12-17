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

package com.spotify.helios.servicescommon;

import static org.junit.Assert.assertEquals;

import com.google.common.base.Charsets;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ResolverConfReaderTest {
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void test() throws Exception {
    final File f = temporaryFolder.newFile();
    final String absolutePath = f.getAbsolutePath();
    final FileOutputStream fos = new FileOutputStream(absolutePath);
    final ByteBuffer bytes = Charsets.UTF_8.encode(
        "nameserver 127.0.0.1\nsearch spotify.com\ndomain foo.bar\n");
    fos.write(bytes.array());
    fos.close();
    assertEquals("foo.bar", ResolverConfReader.getDomainFromResolverConf(absolutePath));
  }

  @Test
  public void testFileNotFound() throws Exception {
    assertEquals("", ResolverConfReader.getDomainFromResolverConf(
        "/this_file_really-ShoUldnt=exist"));
  }
}
