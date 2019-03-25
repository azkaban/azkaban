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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

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
    this.cacheDir = spy(this.temporaryFolder.newFolder("projects"));

    final long TEN_MB_IN_BYTE = 10 * 1024 * 1024;
    when(this.cacheDir.getTotalSpace()).thenReturn(TEN_MB_IN_BYTE);

    final ClassLoader classLoader = getClass().getClassLoader();

    final long current = System.currentTimeMillis();
    unzip(Paths.get(classLoader.getResource("1.1.zip").toURI()),
        this.cacheDir.toPath());
    Files.setLastModifiedTime(Paths.get(this.cacheDir.toString(), "1.1",
        FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME), FileTime.fromMillis(current - 2000));

    unzip(Paths.get(classLoader.getResource("2.1.zip").toURI()),
        this.cacheDir.toPath());
    Files.setLastModifiedTime(Paths.get(this.cacheDir.toString(), "2.1",
        FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME), FileTime.fromMillis(current - 1000));

    unzip(Paths.get(classLoader.getResource("3.1.zip").toURI()),
        this.cacheDir.toPath());
    Files.setLastModifiedTime(Paths.get(this.cacheDir.toString(), "3.1",
        FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME), FileTime.fromMillis(current));
  }

  @Test
  /**
   * There's still space in the cache, no deletion.
   */
  public void testNotDeleting() {
    final ProjectCacheCleaner cleaner = new ProjectCacheCleaner(this.cacheDir,
        0.7);
    cleaner.deleteProjectDirsIfNecessary(1);

    assertThat(this.cacheDir.list()).hasSize(3);
  }

  @Test
  /**
   * Deleting everything in the cache to accommodate new item.
   */
  public void testDeletingAll() {
    final ProjectCacheCleaner cleaner = new ProjectCacheCleaner(this.cacheDir, 0.3);
    cleaner.deleteProjectDirsIfNecessary(7000000);

    assertThat(this.cacheDir.list()).hasSize(0);
  }

  @Test
  /**
   * Deleting two least recently used items in the cache to accommodate new item.
   */
  public void testDeletingTwoLRUItems() {
    final ProjectCacheCleaner cleaner = new ProjectCacheCleaner(this.cacheDir, 0.7);
    cleaner.deleteProjectDirsIfNecessary(3000000);
    assertThat(this.cacheDir.list()).hasSize(1);
    assertThat(this.cacheDir.list()).contains("3.1");
  }

  @Test
  /**
   * Deleting the least recently used item in the cache to accommodate new item.
   */
  public void testDeletingOneLRUItem() {
    final ProjectCacheCleaner cleaner = new ProjectCacheCleaner(this.cacheDir, 0.7);
    cleaner.deleteProjectDirsIfNecessary(2000000);
    assertThat(this.cacheDir.list()).hasSize(2);
    assertThat(this.cacheDir.list()).contains("3.1");
    assertThat(this.cacheDir.list()).contains("2.1");
  }
}
