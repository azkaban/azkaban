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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import azkaban.AzkabanCommonModuleConfig;
import azkaban.spi.DependencyFile;
import azkaban.spi.FileIOStatus;
import azkaban.spi.ProjectStorageMetadata;
import azkaban.test.executions.ThinArchiveTestUtils;
import azkaban.utils.HashUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class LocalStorageTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  static final String SAMPLE_FILE = "sample_flow_01.zip";
  static File BASE_DIRECTORY;

  private static final Logger log = Logger.getLogger(LocalStorageTest.class);

  private LocalStorage localStorage;

  @Before
  public void setUp() throws Exception {
    BASE_DIRECTORY = TEMP_DIR.newFolder("TEST_LOCAL_STORAGE");

    final AzkabanCommonModuleConfig config = mock(AzkabanCommonModuleConfig.class);

    when(config.getLocalStorageBaseDirPath()).thenReturn(BASE_DIRECTORY.getCanonicalPath());
    this.localStorage = new LocalStorage(config);
  }

  @Test
  public void testPutGetDeleteProject() throws Exception {
    final ClassLoader classLoader = getClass().getClassLoader();
    final File testFile = new File(classLoader.getResource(SAMPLE_FILE).getFile());

    final ProjectStorageMetadata metadata = new ProjectStorageMetadata(
        1, 1, "testuser", HashUtils.MD5.getHashBytes(testFile));
    final String key = this.localStorage.putProject(metadata, testFile);
    assertNotNull(key);
    log.info("Key URI: " + key);

    final File expectedTargetFile = new File(BASE_DIRECTORY, new StringBuilder()
        .append(metadata.getProjectId())
        .append(File.separator)
        .append(metadata.getProjectId())
        .append("-")
        .append(new String(Hex.encodeHex(metadata.getHash())))
        .append(".zip")
        .toString()
    );
    assertTrue(expectedTargetFile.exists());
    assertTrue(FileUtils.contentEquals(testFile, expectedTargetFile));

    // test get
    final InputStream getIs = this.localStorage.getProject(key);
    assertNotNull(getIs);
    final File getFile = new File("tmp.get");
    FileUtils.copyInputStreamToFile(getIs, getFile);
    assertTrue(FileUtils.contentEquals(testFile, getFile));

    // Cleanup temp file
    getFile.delete();

    assertTrue(this.localStorage.deleteProject(key));
    boolean exceptionThrown = false;
    try {
      this.localStorage.getProject(key);
    } catch (final FileNotFoundException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  @Test
  public void testPutGetExistsDependency() throws Exception {
    final File tmpJar = TEMP_DIR.newFile(ThinArchiveTestUtils.getDepA().getFileName());
    FileUtils.writeStringToFile(tmpJar, ThinArchiveTestUtils.getDepAContent());
    DependencyFile depFile = ThinArchiveTestUtils.getDepA().makeDependencyFile(tmpJar);

    assertEquals(FileIOStatus.CLOSED, this.localStorage.putDependency(depFile));
    final File expectedTargetFile = new File(BASE_DIRECTORY, LocalStorage.DEPENDENCY_FOLDER
        + File.separator + ThinArchiveTestUtils.getDepAPath());

    assertTrue(expectedTargetFile.exists());

    final InputStream is =
        this.localStorage.getDependency(ThinArchiveTestUtils.getDepA());

    BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    String fileContent = br.lines().collect(Collectors.joining(System.lineSeparator()));

    assertEquals(ThinArchiveTestUtils.getDepAContent(), fileContent);

    assertEquals(FileIOStatus.CLOSED, this.localStorage.dependencyStatus(ThinArchiveTestUtils.getDepA()));

    // Test that we get FileStatus.NON_EXISTANT for a non-existant dep
    assertEquals(FileIOStatus.NON_EXISTANT, this.localStorage.dependencyStatus(ThinArchiveTestUtils.getDepB()));
  }
}
