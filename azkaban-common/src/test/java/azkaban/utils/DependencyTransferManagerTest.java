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

import azkaban.Constants;
import azkaban.spi.DependencyFile;
import azkaban.spi.Storage;
import azkaban.test.executions.ThinArchiveTestUtils;
import java.io.File;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.stubbing.Answer;

import static azkaban.Constants.ConfigurationKeys.*;
import static azkaban.test.executions.ThinArchiveTestUtils.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


public class DependencyTransferManagerTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private static final int DEPENDENCY_DOWNLOAD_MAX_TRIES = 2;

  private DependencyTransferManager dependencyTransferManager;
  private Storage storage;

  private DependencyFile depA;
  private DependencyFile depB;

  private Set<DependencyFile> depSetAB;
  private Set<DependencyFile> depSetA;
  private static final String WRONG_CONTENT = "WRONG CONTENT!";

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

    Props sampleProps = new Props();
    sampleProps.put(Constants.ConfigurationKeys.AZKABAN_DEPENDENCY_MAX_DOWNLOAD_TRIES, DEPENDENCY_DOWNLOAD_MAX_TRIES);
    sampleProps.put(AZKABAN_DEPENDENCY_DOWNLOAD_TIMEOUT_SECONDS, 3);
    dependencyTransferManager = new DependencyTransferManager(sampleProps, this.storage);
  }

  @Test
  public void testStorageDependencyFetchingDisabled() {
    doReturn(false).when(this.storage).dependencyFetchingEnabled();
    assertFalse(this.storage.dependencyFetchingEnabled());
  }

  @Test
  public void testDownloadEmptySet() throws Exception {
    // Make sure there are no failures when we download with an empty set (it should do nothing)
    this.dependencyTransferManager.downloadAllDependencies(Collections.emptySet(), null);
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
    this.dependencyTransferManager.downloadAllDependencies(depSetAB, null);

    // Assert that the content was written to the files
    assertEquals(ThinArchiveTestUtils.getDepAContent(), FileUtils.readFileToString(depA.getFile()));
    assertEquals(ThinArchiveTestUtils.getDepBContent(), FileUtils.readFileToString(depB.getFile()));
  }

  @Test
  public void testDownloadDependencyHashInvalidOneRetry() throws Exception {
    // When storage.getDependency() is called, return the input stream as if it was downloaded
    // On the first call, return the wrong content to trigger a hash mismatch
    // On the second call, return the correct content
    doReturn(IOUtils.toInputStream(WRONG_CONTENT))
      .doReturn(IOUtils.toInputStream(ThinArchiveTestUtils.getDepAContent())).when(this.storage).getDependency(any());

    // Download ONLY depA
    this.dependencyTransferManager.downloadAllDependencies(depSetA, null);

    verify(this.storage, times(2)).getDependency(depEq(depA));
  }

  @Test(expected = DependencyTransferException.class)
  // Ensure that even if one download fails, we get exception
  public void testDownloadDependencyExceptionForAnyFailure() throws Exception {
    // When getDependency is called, write the content to the file as if it was downloaded
    doAnswer((Answer<InputStream>) invocation -> {
      DependencyFile depFile = (DependencyFile) invocation.getArguments()[0];
      String content = depFile.equals(depA) ? WRONG_CONTENT : ThinArchiveTestUtils.getDepBContent();
      return IOUtils.toInputStream(content);
    }).when(this.storage).getDependency(any());

    this.dependencyTransferManager.downloadAllDependencies(depSetAB, null);
  }

  @Test
  public void testDownloadDependencyTimeoutException() {
    // test waitForAllToSucceedOrOneToFail method with one completableFuture task timeout
    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
      try {
        Thread.sleep(60000);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    });
    CompletableFuture<Void>[] futures = new CompletableFuture[]{future};

    assertThat(catchThrowable(() -> this.dependencyTransferManager.waitForAllToSucceedOrOneToFail(futures)))
        .isInstanceOf(TimeoutException.class);
  }

  // test cancel operation itself won't throw CancellationException to break the whole loop.
  @Test
  public void testCancelCompletableFuture() {
    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
      try {
        Thread.sleep(600000);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    });
    CompletableFuture<Void>[] futures = new CompletableFuture[]{future};
    try {
      dependencyTransferManager.cancelPendingTasks(futures, "projectName");
      // verify if the future is cancelled before completed
      assertThat(future.isCancelled()).isTrue();
    } catch (CancellationException e) {
      fail("Should not throw CancellationException", e);
    }
  }
}
