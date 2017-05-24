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

package azkaban.storage;

import azkaban.AzkabanCommonModuleConfig;
import azkaban.spi.StorageMetadata;
import azkaban.utils.Md5Hasher;
import java.io.File;
import java.io.InputStream;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class LocalStorageTest {
  private static final Logger log = Logger.getLogger(LocalStorageTest.class);

  static final String SAMPLE_FILE = "sample_flow_01.zip";
  static final String LOCAL_STORAGE = "LOCAL_STORAGE";
  static final File BASE_DIRECTORY = new File(LOCAL_STORAGE);

  private LocalStorage localStorage;

  @Before
  public void setUp() throws Exception {
    tearDown();
    BASE_DIRECTORY.mkdir();
    AzkabanCommonModuleConfig config = mock(AzkabanCommonModuleConfig.class);
    when(config.getLocalStorageBaseDirPath()).thenReturn(LOCAL_STORAGE);
    localStorage = new LocalStorage(config);
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(BASE_DIRECTORY);
  }

  @Test
  public void testPutGet() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    File testFile = new File(classLoader.getResource(SAMPLE_FILE).getFile());

    final StorageMetadata metadata = new StorageMetadata(
        1, 1, "testuser", Md5Hasher.md5Hash(testFile));
    final String key = localStorage.put(metadata, testFile);
    assertNotNull(key);
    log.info("Key URI: " + key);

    File expectedTargetFile = new File(BASE_DIRECTORY, new StringBuilder()
        .append(metadata.getProjectId())
        .append(File.separator)
        .append(metadata.getProjectId())
        .append("-")
        .append(new String(metadata.getHash()))
        .append(".zip")
        .toString()
    );
    assertTrue(expectedTargetFile.exists());
    assertTrue(FileUtils.contentEquals(testFile, expectedTargetFile));

    // test get
    InputStream getIs = localStorage.get(key);
    assertNotNull(getIs);
    File getFile = new File("tmp.get");
    FileUtils.copyInputStreamToFile(getIs, getFile);
    assertTrue(FileUtils.contentEquals(testFile, getFile));
    getFile.delete();
  }
}
