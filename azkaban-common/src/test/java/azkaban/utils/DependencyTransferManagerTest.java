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
import azkaban.spi.FileIOStatus;
import azkaban.spi.FileOrigin;
import azkaban.spi.Storage;
import azkaban.storage.HdfsStorage;
import azkaban.storage.LocalStorage;
import azkaban.test.executions.ThinArchiveTestUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static azkaban.Constants.ConfigurationKeys.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


@RunWith(PowerMockRunner.class)
@PrepareForTest(FileUtils.class)
public class DependencyTransferManagerTest {
  @Rule
  private final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private final String DOWNLOAD_BASE_URL = "https://example.com/";
  private final String HDFS_DEPENDENCY_ROOT_URI = "https://localhost:8000/deps/";

  private DependencyTransferManager dependencyTransferManager;
  private Storage storage;
  private Props props;

  private URL depAFullUrl;
  private DependencyFile depA;

  private URL depBFullUrl;
  private DependencyFile depB;

  private Set<DependencyFile> depSetAB;
  private Set<DependencyFile> depSetA;

  @Before
  public void setup() throws Exception {
    this.props = new Props();
    this.props.put(AZKABAN_STARTUP_DEPENDENCIES_REMOTE_DOWNLOAD_BASE_URL, DOWNLOAD_BASE_URL);
    this.storage = mock(LocalStorage.class);

    File tempFolder = TEMP_DIR.newFolder("tmpproj");

    depAFullUrl = new URL(new URL(DOWNLOAD_BASE_URL), ThinArchiveTestUtils.getDepAPath());
    depA = ThinArchiveUtils.getDependencyFile(tempFolder, ThinArchiveTestUtils.getDepA());

    depBFullUrl = new URL(new URL(DOWNLOAD_BASE_URL), ThinArchiveTestUtils.getDepBPath());
    depB = ThinArchiveUtils.getDependencyFile(tempFolder, ThinArchiveTestUtils.getDepB());

    depSetAB = new HashSet();
    depSetAB.add(depA);
    depSetAB.add(depB);

    depSetA = new HashSet();
    depSetA.add(depA);

    dependencyTransferManager = new DependencyTransferManager(this.props, this.storage);
  }

  @Test
  public void testAllEnabled() {
    // Default instance created by setup() should have all origins enabled
    assertTrue(this.dependencyTransferManager.storageOriginEnabled());
    assertTrue(this.dependencyTransferManager.remoteOriginEnabled());
  }

  private void ensureOriginDisabled(DependencyTransferManager dtm, FileOrigin o) {
    if (o == FileOrigin.STORAGE) {
      assertFalse(dtm.storageOriginEnabled());
      assertTrue(dtm.remoteOriginEnabled());
    } else {
      assertTrue(dtm.storageOriginEnabled());
      assertFalse(dtm.remoteOriginEnabled());
    }

    // This should succeed
    dtm.downloadAllDependencies(Collections.emptySet(), FileOrigin.STORAGE);

    // This should fail
    boolean hitException = false;
    try {
      dtm.downloadAllDependencies(this.depSetAB, o);
    } catch (UnsupportedOperationException e) {
      hitException = true;
    }
    if (!hitException) fail("Expected UnsuportedOperationException.");

    // This should also fail
    hitException = false;
    try {
      dtm.uploadAllDependencies(this.depSetAB, o);
    } catch (UnsupportedOperationException e) {
      hitException = true;
    }
    if (!hitException) fail("Expected UnsuportedOperationException.");
  }

  @Test
  public void testStorageDisabled() {
    // Properly set download URL, but don't set root dependency URI for HDFS storage.
    Props tmpProps = new Props();
    tmpProps.put(AZKABAN_STARTUP_DEPENDENCIES_REMOTE_DOWNLOAD_BASE_URL, DOWNLOAD_BASE_URL);
    Storage tmpStorage = mock(HdfsStorage.class);
    DependencyTransferManager dtm = new DependencyTransferManager(tmpProps, tmpStorage);

    ensureOriginDisabled(dtm, FileOrigin.STORAGE);
  }

  @Test
  public void testLocalStorageEnabled() {
    // Properly set download URL, but don't set root dependency URI (we don't need it for LocalStorage)
    Props tmpProps = new Props();
    tmpProps.put(AZKABAN_STARTUP_DEPENDENCIES_REMOTE_DOWNLOAD_BASE_URL, DOWNLOAD_BASE_URL);
    Storage tmpStorage = mock(LocalStorage.class);
    DependencyTransferManager dtm = new DependencyTransferManager(tmpProps, tmpStorage);

    assertTrue(dtm.storageOriginEnabled());
    assertTrue(dtm.remoteOriginEnabled());
  }

  @Test
  public void testRemoteDisabled() {
    // Properly set root dependency URI for HDFS storage, but not download URL
    Props tmpProps = new Props();
    tmpProps.put(AZKABAN_STORAGE_HDFS_DEPENDENCY_ROOT_URI, HDFS_DEPENDENCY_ROOT_URI);
    Storage tmpStorage = mock(HdfsStorage.class);
    DependencyTransferManager dtm = new DependencyTransferManager(tmpProps, tmpStorage);

    ensureOriginDisabled(dtm, FileOrigin.REMOTE);
  }

  @Test
  public void testUploadDependencySuccessSTORAGE() throws Exception {
    // Indicate both dependencies persisted successfully
    when(this.storage.putDependency(depA)).thenReturn(FileIOStatus.CLOSED);
    when(this.storage.putDependency(depB)).thenReturn(FileIOStatus.CLOSED);

    // Both dependencies were indicated to be finalized as CLOSED so they should both be returned
    // because we can guarantee that they are successfully persisted in storage
    assertEquals(depSetAB, this.dependencyTransferManager.uploadAllDependencies(depSetAB, FileOrigin.STORAGE));

    verify(this.storage).putDependency(depA);
    verify(this.storage).putDependency(depB);
  }

  @Test
  public void testUploadDependencyOneClosedOneOpenSTORAGE() throws Exception {
    // Indicate depA persisted successfully, and depB may still be OPEN
    when(this.storage.putDependency(depA)).thenReturn(FileIOStatus.CLOSED);
    when(this.storage.putDependency(depB)).thenReturn(FileIOStatus.OPEN);

    // ONLY depA was indicated to be successfully persisted as closed, so we only expect depA to be returned
    // as guaranteed persisted
    assertEquals(depSetA, this.dependencyTransferManager.uploadAllDependencies(depSetAB, FileOrigin.STORAGE));

    verify(this.storage).putDependency(depA);
    verify(this.storage).putDependency(depB);
  }

  @Test
  public void testUploadEmptySet() throws Exception {
    // Make sure we get back an empty set when persisting nothing
    assertEquals(Collections.emptySet(),
        this.dependencyTransferManager.uploadAllDependencies(Collections.emptySet(), FileOrigin.STORAGE));
  }

  @Test
  public void testDownloadEmptySet() throws Exception {
    // Make sure there are no failures when we download with an empty set (it should do nothing)
    this.dependencyTransferManager.downloadAllDependencies(Collections.emptySet(), FileOrigin.STORAGE);
  }

  @Test(expected = DependencyTransferException.class)
  public void testUploadDependencyFailSTORAGE() throws Exception {
    // Indicate depA persisted successfully, but we get an IOException when we attempt to persist depB.
    when(this.storage.putDependency(depA)).thenReturn(FileIOStatus.CLOSED);
    when(this.storage.putDependency(depB)).thenThrow(new IOException());

    // Download depA and depB
    this.dependencyTransferManager.uploadAllDependencies(depSetAB, FileOrigin.STORAGE);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUploadDependencyINVALID_ORIGIN() throws Exception {
    // We can't upload to REMOTE! We can only upload to STORAGE.
    this.dependencyTransferManager.uploadAllDependencies(depSetAB, FileOrigin.REMOTE);
  }

  @Test
  public void testDownloadDependencySuccessREMOTE() throws Exception {
    PowerMockito.spy(FileUtils.class);

    // When FileDownloaderUtils.downloadToFile() is called, write the content to the file as if it was downloaded
    PowerMockito.doAnswer((Answer) invocation -> {
      URL remoteURL = (URL) invocation.getArguments()[0];
      File destFile = (File) invocation.getArguments()[1];
      String content = remoteURL.equals(depAFullUrl) ? ThinArchiveTestUtils.getDepAContent() : ThinArchiveTestUtils.getDepBContent();
      FileUtils.writeStringToFile(destFile, content);
      return null;
    }).when(FileUtils.class, "copyURLToFile", Mockito.any(URL.class), Mockito.any(), Mockito.anyInt(), Mockito.anyInt());

    // Download depA and depB
    this.dependencyTransferManager.downloadAllDependencies(depSetAB, FileOrigin.REMOTE);

    PowerMockito.verifyStatic(FileUtils.class, Mockito.times(1));
    FileUtils.copyURLToFile(Mockito.eq(depAFullUrl), Mockito.eq(depA.getFile()), Mockito.anyInt(), Mockito.anyInt());

    PowerMockito.verifyStatic(FileUtils.class, Mockito.times(1));
    FileUtils.copyURLToFile(Mockito.eq(depBFullUrl), Mockito.eq(depB.getFile()), Mockito.anyInt(), Mockito.anyInt());
  }

  @Test
  public void testDownloadDependencySuccessSTORAGE() throws Exception {
    // When FileDownloaderUtils.downloadToFile() is called, write the content to the file as if it was downloaded
    doAnswer((Answer<InputStream>) invocation -> {
      DependencyFile depFile = (DependencyFile) invocation.getArguments()[0];
      String filename;
      String content;
      if (depFile.equals(depA)) {
        filename = "tmpdepa.jar";
        content = ThinArchiveTestUtils.getDepAContent();
      } else {
        filename = "tmpdepb.jar";
        content = ThinArchiveTestUtils.getDepBContent();
      }

      File tmpFile = TEMP_DIR.newFile(filename);
      FileUtils.writeStringToFile(tmpFile, content);
      return new FileInputStream(tmpFile);
    }).when(this.storage).getDependency(Mockito.any());

    // Download depA and depB
    this.dependencyTransferManager.downloadAllDependencies(depSetAB, FileOrigin.STORAGE);

    verify(this.storage).getDependency(depA);
    verify(this.storage).getDependency(depB);
  }

  @Test
  public void testDownloadDependencyHashInvalidOneRetryREMOTE() throws Exception {
    final AtomicInteger countCall = new AtomicInteger();
    PowerMockito.spy(FileUtils.class);

    // When FileDownloaderUtils.downloadToFile() is called, write the content to the file as if it was downloaded
    PowerMockito.doAnswer((Answer) invocation -> {
      File destFile = (File) invocation.getArguments()[1];

      if (countCall.getAndIncrement() == 0) {
        // On the first call, write the wrong content to trigger a hash mismatch
        FileUtils.writeStringToFile(destFile, "WRONG CONTENT!!!!!!!");
      } else {
        FileUtils.writeStringToFile(destFile, ThinArchiveTestUtils.getDepAContent());
      }
      return null;
    }).when(FileUtils.class, "copyURLToFile", Mockito.any(URL.class), Mockito.eq(depA.getFile()), Mockito.anyInt(), Mockito.anyInt());

    // Download ONLY depA
    this.dependencyTransferManager.downloadAllDependencies(depSetA, FileOrigin.REMOTE);

    PowerMockito.verifyStatic(FileUtils.class, Mockito.times(2));
    FileUtils.copyURLToFile(Mockito.eq(depAFullUrl), Mockito.eq(depA.getFile()), Mockito.anyInt(), Mockito.anyInt());
  }

  @Test
  public void testDownloadDependencyHashInvalidRetryExceededFailREMOTE() throws Exception {
    PowerMockito.spy(FileUtils.class);

    // When FileDownloaderUtils.downloadToFile() is called, write the content to the file as if it was downloaded
    PowerMockito.doAnswer((Answer) invocation -> {
      File destFile = (File) invocation.getArguments()[1];
      FileUtils.writeStringToFile(destFile, "WRONG CONTENT!!!!!!!");
      return null;
    }).when(FileUtils.class, "copyURLToFile", Mockito.any(URL.class), Mockito.eq(depA.getFile()), Mockito.anyInt(), Mockito.anyInt());

    boolean hitException = false;
    try {
      // Download ONLY depA
      this.dependencyTransferManager.downloadAllDependencies(depSetA, FileOrigin.REMOTE);
    } catch (DependencyTransferException e) {
      // Good! We wanted this exception.
      hitException = true;
    }

    if (!hitException) {
      Assert.fail("Expected DependencyTransferException but didn't get any.");
    }

    // We expect the download to be attempted the maximum number of times before it fails
    PowerMockito.verifyStatic(FileUtils.class, Mockito.times(DependencyTransferManager.MAX_DEPENDENCY_DOWNLOAD_TRIES));
    FileUtils.copyURLToFile(Mockito.eq(depAFullUrl), Mockito.eq(depA.getFile()), Mockito.anyInt(), Mockito.anyInt());
  }

  @Test
  // Ensure that if one download fails, we don't wait for the other to complete, but instead we return immediately
  public void testDownloadDependencyFastFailSTORAGE() throws Exception {
    AtomicBoolean stillDownloadingDepB = new AtomicBoolean(false);

    // When FileDownloaderUtils.downloadToFile() is called, write the content to the file as if it was downloaded
    doAnswer((Answer<InputStream>) invocation -> {
      DependencyFile depFile = (DependencyFile) invocation.getArguments()[0];
      String filename;
      String content;
      if (depFile.equals(depA)) {
        // Write the wrong content for depA, triggering a HashNotMatchException
        filename = "tmpdepa.jar";
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
        filename = "tmpdepb.jar";
        content = ThinArchiveTestUtils.getDepBContent();
      }

      File tmpFile = TEMP_DIR.newFile(filename);
      FileUtils.writeStringToFile(tmpFile, content);
      return new FileInputStream(tmpFile);
    }).when(this.storage).getDependency(Mockito.any());

    boolean hitException = false;
    try {
      // Download both depA and depB
      this.dependencyTransferManager.downloadAllDependencies(depSetAB, FileOrigin.STORAGE);
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
    verify(this.storage, times(DependencyTransferManager.MAX_DEPENDENCY_DOWNLOAD_TRIES)).getDependency(depA);
    // depB may or may not have been attempted to be downloaded depending on if the thread pool
    // was shutdown before or after it reached its call to storage.getDependency
  }
}
