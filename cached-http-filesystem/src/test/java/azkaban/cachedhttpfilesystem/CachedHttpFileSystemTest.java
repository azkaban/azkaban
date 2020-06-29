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
 *
 */

package azkaban.cachedhttpfilesystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.internal.util.reflection.FieldSetter;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * These tests use CachedHttpFileSystem with a mocked HTTP backend and a real Hadoop LocalFileSystem for caching.
 */
public class CachedHttpFileSystemTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private static final String JAR_CONTENT = "blistering barnacles!";
  private static final String RELATIVE_JAR_PATH = "jars/some.jar";

  private URI chttpRootURI;
  private URI cacheRootURI;
  private File localCacheFolder;
  private File expectedLocallyCachedJar;

  private URI originAbsoluteJarURI;
  private URI chttpAbsoluteJarURI;

  public CachedHttpFileSystem cachedHttpFileSystem;
  public FileSystem cacheFS;

  @Before
  public void setup() throws Exception {
    // So log4j doesn't complain
    BasicConfigurator.configure();

    this.localCacheFolder = TEMP_DIR.newFolder("dependencies");
    this.chttpRootURI = URI.create("chttp://www.example.com:9000/repo/");
    this.cacheRootURI = URI.create("file://" + localCacheFolder.getCanonicalPath());

    this.chttpAbsoluteJarURI = this.chttpRootURI.resolve(RELATIVE_JAR_PATH);
    URIBuilder b = new URIBuilder(this.chttpAbsoluteJarURI);
    b.setScheme("http");
    // http://www.example.com:9000/repo/jars/some.jar
    this.originAbsoluteJarURI = b.build();

    this.expectedLocallyCachedJar = new File(this.localCacheFolder, RELATIVE_JAR_PATH);

    this.cacheFS = mock(FileSystem.class);

    Configuration conf = new Configuration(false);
    // Stop FileSystem.get() from returning cached instances of CachedHttpFileSystem, it messes with our tests.
    // we need to create a new object each time.
    conf.setBoolean("fs.chttp.impl.disable.cache", true);
    conf.set("fs.chttp.impl", azkaban.cachedhttpfilesystem.CachedHttpFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    conf.set(CachedHttpFileSystem.CACHE_ROOT_URI, this.cacheRootURI.toString());

    this.cachedHttpFileSystem = spy((CachedHttpFileSystem) FileSystem.get(chttpRootURI, conf));
    configureMockResponseFromHttpOrigin(this.cachedHttpFileSystem, this.originAbsoluteJarURI);
  }

  private static void configureMockResponseFromHttpOrigin(CachedHttpFileSystem fs, URI originAbsoluteJarURI)
      throws Exception {
    // Intercept the internal method call to request a file from a HTTP URL and return our own input stream
    // instead.
    doReturn(stringToFSDataInputStream(JAR_CONTENT))
        .when(fs).downloadFromOrigin(originAbsoluteJarURI);
  }

  private static FSDataInputStream stringToFSDataInputStream(String str) {
    return new FSDataInputStream(new CachedHttpFileSystem.HttpDataInputStream(IOUtils.toInputStream(str)));
  }

  private void assertInputStreamIsJarContent(InputStream inp) throws Exception {
    // Ensure the InputStream resolves to the content of our sample jar.
    assertEquals(JAR_CONTENT, IOUtils.toString(inp));
  }

  @Test
  public void testCachingDisabled() throws Exception {
    Configuration conf = new Configuration(false);
    // Stop FileSystem.get() from returning cached instances of CachedHttpFileSystem, it messes with our tests.
    // we need to create a new object each time.
    conf.setBoolean("fs.chttp.impl.disable.cache", true);
    conf.set("fs.chttp.impl", azkaban.cachedhttpfilesystem.CachedHttpFileSystem.class.getName());
    conf.set(CachedHttpFileSystem.CACHE_ENABLED_FLAG, "false");

    CachedHttpFileSystem tmpCachedHttpFileSystem = spy((CachedHttpFileSystem) FileSystem.get(chttpRootURI, conf));
    configureMockResponseFromHttpOrigin(tmpCachedHttpFileSystem, this.originAbsoluteJarURI);

    // Make sure we get the content back from the origin as expected
    assertInputStreamIsJarContent(tmpCachedHttpFileSystem.open(new Path(this.chttpAbsoluteJarURI)));
  }

  @Test(expected = IOException.class)
  public void testInvalidUriPath() throws Exception {
    // This URI cannot be relativized against chttp://www.example.com:9000/repo/
    this.cachedHttpFileSystem.open(new Path("chttp://www.wrongurl.com/some.jar"), 0);
  }

  @Test(expected = IOException.class)
  public void testInvalidUriScheme() throws Exception {
    // This URI cannot be relativized against chttp://www.example.com:9000/repo/ (wrong scheme)
    this.cachedHttpFileSystem.open(new Path("http://www.wrongurl.com/repo/some.jar"), 0);
  }

  @Test
  public void testCacheMissThenHit() throws Exception {
    // This should be a cache miss, and populate the cache
    assertInputStreamIsJarContent(this.cachedHttpFileSystem.open(new Path(this.chttpAbsoluteJarURI)));

    // Because this was a cache miss, we expect the file queried from origin once.
    verify(this.cachedHttpFileSystem).downloadFromOrigin(any());

    // We expect the jar file to be populated in the correct place in the cache
    assertTrue(this.expectedLocallyCachedJar.exists());

    // **** SECOND REQUEST TO CACHEDHTTPFILESYSTEM ****
    // This should be a cache hit
    assertInputStreamIsJarContent(this.cachedHttpFileSystem.open(new Path(this.chttpAbsoluteJarURI)));

    // Because this was a cache hit, we do NOT expect any additional queries to the origin (still only one from
    // the first call)
    verify(this.cachedHttpFileSystem).downloadFromOrigin(any());
  }

  @Test
  public void testCacheMissFailPersist() throws Exception {
    // Test the scenario where we don't find the object in the cache, but we fail to persist the object into the cache.
    // We should still get back an FSDataInputStream for the object from the origin.

    // This is a little brittle, but we have to peek inside CachedHttpFileSystem and inject our own mocked cacheFS
    FileSystem mockedFS = mock(FileSystem.class);
    // when CachedHttpFileSystem attempts to see if the file is already cached - throw FileNotFoundException (simulate
    // a cache miss)
    doThrow(new FileNotFoundException()).when(mockedFS).open(any(), anyInt());
    // when CachedHttpFileSystem attempts to create the tmp file - we throw an exception.
    doThrow(new IOException()).when(mockedFS).create(any(), anyBoolean());
    FieldSetter.setField(this.cachedHttpFileSystem, CachedHttpFileSystem.class.getDeclaredField("cacheFS"), mockedFS);

    // This should be a cache miss BUT should still succeed (the error opening a stream to the tmp file should be
    // swallowed)
    assertInputStreamIsJarContent(this.cachedHttpFileSystem.open(new Path(this.chttpAbsoluteJarURI)));

    // We do NOT expect the jar file to be persisted, because the persist failed!
    assertFalse(this.expectedLocallyCachedJar.exists());
  }

  @Test
  public void testCacheMissRaceCondition() throws Exception {
    // Test the scenario where we don't find the object in the cache, we download from origin, write to tmp file
    // but when we attempt to rename the tmp file, the rename fails because another process already persisted
    // this file to cache.

    doAnswer(i -> {
      // Before returning the input stream, write the file into the cache as if another process pulling the same
      // file from CachedHttpFileSystem beat us to the race in persisting to cache. This will result in our process
      // failing to rewrite the tmp file to the final file, but it should still return the InputStream just the same.
      FileUtils.writeStringToFile(this.expectedLocallyCachedJar, JAR_CONTENT);
      return stringToFSDataInputStream(JAR_CONTENT);
    }).when(this.cachedHttpFileSystem).downloadFromOrigin(this.originAbsoluteJarURI);

    // This should be a cache miss BUT should still succeed (the error renaming the file should be swallowed)
    assertInputStreamIsJarContent(this.cachedHttpFileSystem.open(new Path(this.chttpAbsoluteJarURI)));
  }
}
