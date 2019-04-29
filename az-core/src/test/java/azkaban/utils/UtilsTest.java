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
import org.apache.commons.lang.SystemUtils;
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
    final String[] LS_COMMAND = SystemUtils.IS_OS_WINDOWS
        ? new String[]{"cmd", "/c", "dir"}
        : new String[]{"/bin/bash", "-c", "ls"};
    final ArrayList<String> result = Utils.runProcess(LS_COMMAND);
    Assert.assertNotEquals(result.size(), 0);
  }
}
