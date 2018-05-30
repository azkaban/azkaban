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
import java.io.IOException;
import java.util.zip.ZipFile;
import org.junit.Test;

/**
 * Test class for azkaban.utils.Utils
 */
public class UtilsTest {

  private static final String INVALID_ZIP_FILE = "zip-slip.zip";

  @Test
  public void testUnzipInvalidFile() throws IOException {
    final File zipFile = new File(
        getClass().getClassLoader().getResource(INVALID_ZIP_FILE).getFile());
    final ZipFile source = new ZipFile(zipFile);
    final File dest = Utils.createTempDir();
    assertThatThrownBy(() -> Utils.unzip(source, dest)).isInstanceOf(IOException.class)
        .hasMessageContaining("Extracting Zip entry would have resulted in a file outside the "
            + "specified destination directory.");
  }
}
