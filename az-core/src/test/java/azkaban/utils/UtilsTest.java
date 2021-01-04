/*
 * Copyright 2018 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the “License”); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.utils;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test class for azkaban.utils.Utils
 */
public class UtilsTest {

  /**
   * An insecure zip file may hold path traversal filenames. During unzipping, the filename gets
   * concatenated to the target directory. The final path may end up outside the target directory,
   * causing security issues.
   *
   * @throws IOException the io exception
   */
  @Test
  public void testUnzipInsecureFile() throws IOException {
    final File zipFile = new File("myTest.zip");
    try {
      try (final ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipFile))) {
        final ZipEntry entry = new ZipEntry("../../../../../evil.txt");
        out.putNextEntry(entry);
      }

      final ZipFile source = new ZipFile(zipFile);
      final File dest = Utils.createTempDir();
      assertThatThrownBy(() -> Utils.unzip(source, dest)).isInstanceOf(IOException.class)
          .hasMessageContaining("Extracting zip entry would have resulted in a file outside the "
              + "specified destination directory.");
    } finally {
      if (zipFile.exists()) {
        zipFile.delete();
      }
    }
  }

  @Test
  public void testRunProcess() throws IOException, InterruptedException {
    ArrayList<String> result =
        Utils.runProcess("/bin/bash", "-c", "ls");
    Assert.assertNotEquals(result.size(), 0);
  }


  @Test
  public void testMemoryStringConversion() {
    Assert.assertEquals(Utils.parseMemString("1024"), 1L);
    Assert.assertEquals(Utils.parseMemString("1K"), 1L);
    Assert.assertEquals(Utils.parseMemString("1M"), 1024L);
    Assert.assertEquals(Utils.parseMemString("1G"), 1024L * 1024L);

    Assert.assertEquals(Utils.parseMemString("1k"), 1L);
    Assert.assertEquals(Utils.parseMemString("1m"), 1024L);
    Assert.assertEquals(Utils.parseMemString("1g"), 1024L * 1024L);

    Assert.assertEquals(Utils.parseMemString("5000"), 4L);
    Assert.assertEquals(Utils.parseMemString("1024K"), 1024L);
    Assert.assertEquals(Utils.parseMemString("512M"), 512 * 1024L);
    Assert.assertEquals(Utils.parseMemString("2G"), 2L * 1024L * 1024L);
  }

  @Test
  public void testBadMemoryStringFormat() {
    badMemoryStringFormatHelper("1KB");
    badMemoryStringFormatHelper("1MB");
    badMemoryStringFormatHelper("1GB");

    badMemoryStringFormatHelper("1kb");
    badMemoryStringFormatHelper("1mb");
    badMemoryStringFormatHelper("1gb");

    badMemoryStringFormatHelper("100.5K");
    badMemoryStringFormatHelper("512.8M");
    badMemoryStringFormatHelper("0.5G");

    badMemoryStringFormatHelper("100b");
    badMemoryStringFormatHelper("100f");
    badMemoryStringFormatHelper("100abcdc");
  }

  private void badMemoryStringFormatHelper(final String str) {
    try {
      Utils.parseMemString(str);
      Assert.fail("should get a runtime exception");
    } catch (final Exception e) {
      Assert.assertTrue(e instanceof NumberFormatException);
    }
  }
}
