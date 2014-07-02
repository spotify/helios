package com.spotify.helios.servicescommon;

import com.google.common.base.Charsets;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

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
