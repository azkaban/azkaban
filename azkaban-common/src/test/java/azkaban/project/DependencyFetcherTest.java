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

package azkaban.project;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static azkaban.project.DependencyFetcher.*;
import static org.junit.Assert.*;


public class DependencyFetcherTest {
  // Why jquery? because it is a small (158KB) jar.
  public static final String SAMPLE_URL = "http://central.maven.org/maven2/org/webjars/jquery/2.1.4/jquery-2.1.4.jar";
  public static final String SAMPLE_FILENAME = "jquery-2.1.4.jar";
  public static final String SAMPLE_SHA1 = "444C76E4FF085CAC934A668EA52B9E29A40E45E6";
  public static final long   SAMPLE_FILE_SIZE = 158370L;

  private final File tmpDir = new File("fetch-tmp-dir");
  private final ProjectManagerConfig config = new ProjectManagerConfig();
  private final DependencyFetcher instance = new DependencyFetcher(config);

  @Before
  public void setUp() throws Exception {
    config.setFetchTmpDir(tmpDir.toPath());
    if (tmpDir.exists()) {
      tearDown();
    }
    tmpDir.mkdirs();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(tmpDir);
  }

  /**
   * This is more of an integration test which goes through the entire flow of fetching a dependency.
   *
   * @throws Exception
   */
  @Test
  public void testFetchDependency() throws Exception {
    URI uri = new URI(SAMPLE_URL);
    instance.fetchDependency(uri);

    Path target = Paths.get(tmpDir.getAbsolutePath(), SAMPLE_SHA1);
    checkDownloadedFile(target);

    Path targetMeta = Paths.get(tmpDir.getAbsolutePath(), SAMPLE_SHA1 + ".properties");
    assertTrue(Files.exists(targetMeta));

    Properties actualMeta = new Properties();
    actualMeta.load(new FileInputStream(targetMeta.toFile()));

    assertEquals(expectedMeta(), actualMeta);
  }

  @Test
  public void testConstructMetadata() throws Exception {
    URI uri = new URI(SAMPLE_URL);
    Path dest = Paths.get(tmpDir.getAbsolutePath(), "hash.test.data.tmp");
    DependencyFetcher.fetchUri(uri, dest);

    assertEquals(expectedMeta(), instance.constructMetaData(uri, dest));
  }

  @Test
  public void testComputeHash() throws Exception {
    Path dest = Paths.get(tmpDir.getAbsolutePath(), "test.hash.data.tmp");
    DependencyFetcher.fetchUrl(new URL(SAMPLE_URL), dest);

    assertEquals(SAMPLE_SHA1, DependencyFetcher.calcSHA1(dest.toFile()));
    assertEquals(SAMPLE_SHA1, computeHash(dest));
  }

  @Test
  public void testFetchUri() throws Exception {
    Path dest = Paths.get(tmpDir.getAbsolutePath(), "test.uri.data.tmp");
    DependencyFetcher.fetchUri(new URI(SAMPLE_URL), dest);

    checkDownloadedFile(dest);
  }

  @Test
  public void testFetchUrl() throws Exception {
    Path dest = Paths.get(tmpDir.getAbsolutePath(), "test.url.data.tmp");
    DependencyFetcher.fetchUrl(new URL(SAMPLE_URL), dest);

    checkDownloadedFile(dest);
  }

  private static void checkDownloadedFile(Path target) throws IOException {
    assertTrue(Files.exists(target));
    assertEquals(SAMPLE_FILE_SIZE, target.toFile().length());
    assertEquals(SAMPLE_SHA1, computeHash(target));
  }

  private static Properties expectedMeta() throws URISyntaxException {
    Properties expected = new Properties();
    expected.setProperty("uri", new URI(SAMPLE_URL).toString());
    expected.setProperty("filename", SAMPLE_FILENAME);
    expected.setProperty("size", String.valueOf(SAMPLE_FILE_SIZE));
    return expected;
  }
}