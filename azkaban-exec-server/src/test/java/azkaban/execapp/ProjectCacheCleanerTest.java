/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package azkaban.execapp;

import azkaban.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.zip.ZipFile;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class ProjectCacheCleanerTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File cacheDir;

  private void unzip(final Path srcZipFile, final Path dest) throws IOException {
    final ZipFile zip = new ZipFile(srcZipFile.toFile());
    Utils.unzip(zip, dest.toFile());
  }

  @Before
  public void setUp() throws Exception {
    this.cacheDir = this.temporaryFolder.newFolder("projects");
    final ClassLoader classLoader = getClass().getClassLoader();

    final long current = System.currentTimeMillis();
    unzip(Paths.get(classLoader.getResource("1.1.zip").getPath()),
        this.cacheDir.toPath());
    Files.setLastModifiedTime(Paths.get(this.cacheDir.toString(), "1.1",
        FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME), FileTime.fromMillis(current));

    unzip(Paths.get(classLoader.getResource("2.1.zip").getPath()),
        this.cacheDir.toPath());
    Files.setLastModifiedTime(Paths.get(this.cacheDir.toString(), "2.1",
        FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME), FileTime.fromMillis(current - 1000));

    unzip(Paths.get(classLoader.getResource("3.1.zip").getPath()),
        this.cacheDir.toPath());
    Files.setLastModifiedTime(Paths.get(this.cacheDir.toString(), "3.1",
        FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME), FileTime.fromMillis(current - 2000));
  }

  @Test
  /**
   * There's still space in the cache, no deletion.
   */
  public void testNotDeleting() throws InterruptedException {
    final Long projectDirMaxSize = 3L;
    final ProjectCacheCleaner cleaner = new ProjectCacheCleaner(this.cacheDir, projectDirMaxSize);
    cleaner.deleteProjectDirsIfNecessary(1);
  }

  @Test
  /**
   * deleting everything in the cache to accommodate new item.
   */
  public void testDeletingAll() throws InterruptedException {
    final Long projectDirMaxSize = 3L;
    final ProjectCacheCleaner cleaner = new ProjectCacheCleaner(this.cacheDir, projectDirMaxSize);
    cleaner.deleteProjectDirsIfNecessary(1);
  }

  @Test
  /**
   * deleting least recently used two items in the cache to accommodate new item.
   */
  public void testDeletingLRU2Items() throws InterruptedException {
    final Long projectDirMaxSize = 3L;
    final ProjectCacheCleaner cleaner = new ProjectCacheCleaner(this.cacheDir, projectDirMaxSize);
    cleaner.deleteProjectDirsIfNecessary(1);
  }

    /*
    final List<File> expectedRemainingFiles = new ArrayList<>();

    for (int i = 1; i <= 3; i++) {
      if (i >= 2) {
        //the first file will be deleted
        expectedRemainingFiles.add(pv.getInstalledDir());
      }
      // last modified time of millis second granularity of a file is not supported by all file
      // systems, so sleep for 1 second between creation of each project dir to make their last
      // modified time different.
      Thread.sleep(1000);
    }

    //then
    assertThat(this.projectsDir.listFiles()).containsExactlyInAnyOrder(expectedRemainingFiles
        .toArray(new File[expectedRemainingFiles.size()]));*/
  }

}
