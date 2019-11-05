/*
 * Copyright 2019 LinkedIn Corp.
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
 */

package azkaban.utils;

import azkaban.spi.DependencyFile;
import azkaban.spi.Storage;
import azkaban.test.executions.ThinArchiveTestUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.stubbing.Answer;

import static azkaban.test.executions.ThinArchiveTestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


public class DependencyTransferManagerTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private DependencyTransferManager dependencyTransferManager;
  private Storage storage;

  private DependencyFile depA;
  private DependencyFile depB;

  private Set<DependencyFile> depSetAB;
  private Set<DependencyFile> depSetA;

  @Before
  public void setup() throws Exception {
    this.storage = mock(Storage.class);
    doReturn(true).when(this.storage).dependencyFetchingEnabled();

    File tempFolder = TEMP_DIR.newFolder("tmpproj");

    depA = ThinArchiveUtils.getDependencyFile(tempFolder, ThinArchiveTestUtils.getDepA());
    depB = ThinArchiveUtils.getDependencyFile(tempFolder, ThinArchiveTestUtils.getDepB());

    depSetAB = new HashSet();
    depSetAB.add(depA);
    depSetAB.add(depB);

    depSetA = new HashSet();
    depSetA.add(depA);

    dependencyTransferManager = new DependencyTransferManager(this.storage);
  }

  @Test
  public void testStorageDependencyFetchingDisabled() {
    doReturn(false).when(this.storage).dependencyFetchingEnabled();
    assertFalse(this.storage.dependencyFetchingEnabled());
  }

  @Test
  public void testDownloadEmptySet() throws Exception {
    // Make sure there are no failures when we download with an empty set (it should do nothing)
    this.dependencyTransferManager.downloadAllDependencies(Collections.emptySet());
  }

  @Test
  public void testDownloadDependencySuccess() throws Exception {
    // When storage.getDependency() is called, return the input stream as if it was downloaded
    doAnswer((Answer<InputStream>) invocation -> {
      DependencyFile depFile = (DependencyFile) invocation.getArguments()[0];
      String content = depFile.equals(depA) ? ThinArchiveTestUtils.getDepAContent() : ThinArchiveTestUtils.getDepBContent();
      return IOUtils.toInputStream(content);
    }).when(this.storage).getDependency(any());

    // Download depA and depB
    this.dependencyTransferManager.downloadAllDependencies(depSetAB);

    // Assert that the content was written to the files
    assertEquals(ThinArchiveTestUtils.getDepAContent(), FileUtils.readFileToString(depA.getFile()));
    assertEquals(ThinArchiveTestUtils.getDepBContent(), FileUtils.readFileToString(depB.getFile()));
  }

  @Test
  public void testDownloadDependencyHashInvalidOneRetry() throws Exception {
    // When storage.getDependency() is called, return the input stream as if it was downloaded
    // On the first call, return the wrong content to trigger a hash mismatch
    // On the second call, return the correct content
    doReturn(IOUtils.toInputStream("WRONG CONTENT!!!!!!!"))
      .doReturn(IOUtils.toInputStream(ThinArchiveTestUtils.getDepAContent())).when(this.storage).getDependency(any());

    // Download ONLY depA
    this.dependencyTransferManager.downloadAllDependencies(depSetA);

    verify(this.storage, times(2)).getDependency(depEq(depA));
  }

  @Test
  // Ensure that if one download fails, we don't wait for the other to complete, but instead we return immediately
  public void testDownloadDependencyFastFailRetryExceeded() throws Exception {
    AtomicBoolean stillDownloadingDepB = new AtomicBoolean(false);

    // When getDependency is called, write the content to the file as if it was downloaded
    doAnswer((Answer<InputStream>) invocation -> {
      DependencyFile depFile = (DependencyFile) invocation.getArguments()[0];
      String content;
      if (depFile.equals(depA)) {
        // Write the wrong content for depA, triggering a HashNotMatchException
        content = "WRONG CONTENT!!!!!";
      } else {
        // Take some time
        stillDownloadingDepB.set(true);
        try {
          Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
          stillDownloadingDepB.set(false);
          throw e;
        }
        content = ThinArchiveTestUtils.getDepBContent();
      }

      return IOUtils.toInputStream(content);
    }).when(this.storage).getDependency(any());

    boolean hitException = false;
    try {
      // Download both depA and depB
      this.dependencyTransferManager.downloadAllDependencies(depSetAB);
    } catch (DependencyTransferException e) {
      // Good! We wanted this exception.
      hitException = true;
    }

    if (!hitException) {
      Assert.fail("Expected DependencyTransferException but didn't get any.");
    }

    // Assert that the other thread was interrupted and not allowed to continue
    // we wait for a little while to make sure the other thread has time to exit
    Thread.sleep(500);
    assertFalse(stillDownloadingDepB.get());

    // depA should be attempted to be downloaded the maximum number of times
    verify(this.storage, times(DependencyTransferManager.MAX_DEPENDENCY_DOWNLOAD_TRIES)).getDependency(depEq(depA));
    // depB may or may not have been attempted to be downloaded depending on if the thread pool
    // was shutdown before or after it reached its call to storage.getDependency
  }
}
