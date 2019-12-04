/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package azkaban.cachedhttpfilesystem;

import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.Logger;

import static java.util.Objects.*;


/**
 * NOTE: Some of the boilerplate for the unsupported portions of the API was copied from:
 * https://github.com/apache/hadoop/blob/e346e3638c595a512cd582739ff51fb64c3b4950/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/http/AbstractHttpFileSystem.java
 *
 * CachedHttpFileSystem behaves almost identically to Hadoop's native HttpFileSystem except it adds caching logic to
 * reduce load on the HTTP origin.
 *
 * In fact, CachedHttpFileSystem can be optionally configured to disable caching, in which case it will act EXACTLY
 * like HttpFileSystem. This can be done by setting CACHE_ENABLED_FLAG to FALSE. Each .open() call will request the
 * resource from the origin.
 *
 * If CACHE_ENABLED_FLAG is set to TRUE or not specified (defaults to true):
 *
 * CachedHttpFileSystem expects an additional configuration parameter (value of CACHE_ROOT_URI) to be present
 * which is a full absolute URI that should be resolvable by some other FileSystem (i.e. DistributedFileSystem,
 * LocalFileSystem, etc.). NOTE: This FileSystem must already be configured in the Configuration passed to
 * CachedHttpFileSystem's initialize() method. In addition, if the cache FileSystem requires any authentication,
 * that must happen before .open() on CachedHttpFileSystem is called.
 *
 * When .open() is called: (it must be called with an ABSOLUTE URI!!)
 *
 * 1. CachedHttpFileSystem will first relativize the URI against the "name" URI that CachedHttpFileSystem was
 * initialized with.
 * 2. The relative URI will be resolved against the base cache URI to get an absolute URI for the cache FileSystem
 * 3. The cache FileSystem will be queried to see if the file exists, if it does - it will be returned (skip all further
 * steps).
 * 4. If the file does not exist, the file will be downloaded from the HTTP origin, written into the cache FileSystem
 * and then returned.
 * 5. If the file fails to persist when attempting to be written into the cache, the original InputStream from the HTTP
 * origin will be returned, and the error swallowed.
 *
 * NOTE: To avoid race conditions where multiple processes attempt to persist a file to cache simultaneously, we first
 * write to a temporary file, and then rename the temporary file to the final file name when persisting a file to the
 * cache.
 *
 * FOLDER STRUCTURE ON CACHE EXAMPLE:
 * CachedHttpFileSystem was initialized with URI: chttp://www.example.com/jars/
 * CACHE_ROOT_URI is set to: hdfs://localhost:9000/dependencies/
 *
 * .open() is called on CachedHttpFileSystem with URI: chttp://www.example.com/jars/some/lib/coollib-1.0.0.jar
 * the URI for this file on the cache will be: hdfs://localhost:9000/dependencies/some/lib/coollib-1.0.0.jar
 * and the necessary directories will be created in order to put the file there.
 */
public class CachedHttpFileSystem extends FileSystem {
  // CACHE_ENABLED_FLAG is TRUE by default. When it is true, caching is enabled and the CACHE_ROOT_URI
  // must be specified. If CACHE_ENABLED_FLAG is set to FALSE, caching is disabled and all reads will
  // pull directly from the origin. CACHE_ROOT_URI is not used when caching is disabled.
  public static final String CACHE_ROOT_URI = "cachedhttpfilesystem.cache_root_uri";
  public static final String CACHE_ENABLED_FLAG = "cachedhttpfilesystem.caching_enabled";

  private static final long DEFAULT_BLOCK_SIZE = 4096;
  private static final Path WORKING_DIR = new Path("/");
  private static final String CACHE_TMP_FILE_TEMPLATE = "tmp%d.tmp";
  private static final String HTTP_SCHEME = "http";
  private static final Random RAND = new Random();

  private static final Logger log = Logger.getLogger(CachedHttpFileSystem.class);

  private boolean cachingEnabled;
  private URI uri;
  private FileSystem cacheFS;
  private URI rootCachedURI;
  private URI rootOriginURI;

  @Override
  public void initialize(final URI name, final Configuration conf) throws IOException {
    super.initialize(name, conf);
    this.cachingEnabled = conf.getBoolean(CACHE_ENABLED_FLAG, true);

    this.uri = URI.create(addTrailingForwardSlash(name.toString()));
    URIBuilder rootOriginURIBuilder = new URIBuilder(this.uri);
    rootOriginURIBuilder.setScheme(HTTP_SCHEME);
    try {
      this.rootOriginURI = rootOriginURIBuilder.build();
    } catch (URISyntaxException e) {
      // This should never happen - we are only changing the scheme on an already valid URI.
      throw new RuntimeException(e);
    }

    if (this.cachingEnabled) {
      String cacheRootUri = conf.get(CACHE_ROOT_URI);
      requireNonNull(cacheRootUri);

      this.rootCachedURI = URI.create(addTrailingForwardSlash(cacheRootUri));
      this.cacheFS = FileSystem.get(rootCachedURI, conf);
    }
  }

  // If the base URIs don't have a trailing forward slash the resolving and relativization can get messed up.
  private static String addTrailingForwardSlash(final String in) {
    return in.endsWith("/") ? in : in + "/";
  }

  @Override
  public String getScheme() {
    return "chttp";
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public FSDataInputStream open(final Path path, final int bufferSize) throws IOException {
    URI relativeURI = this.uri.relativize(path.toUri());
    if (relativeURI.isAbsolute()) {
      throw new IOException("Path must be relative or have same prefix as root origin URI.");
    }

    URI resolvedOriginURI = this.rootOriginURI.resolve(relativeURI);
    if (!this.cachingEnabled) {
      // If caching is disabled, just return from the origin.
      this.log.info("CACHE MISS (cache is disabled): " + resolvedOriginURI.toString());
      return downloadFromOrigin(resolvedOriginURI);
    }

    Path resolvedCachePath = new Path(this.rootCachedURI.resolve(relativeURI));
    try {
      // Try to pull from cache
      FSDataInputStream cachedInputStream =  this.cacheFS.open(resolvedCachePath, bufferSize);
      this.log.info("CACHE HIT: " + resolvedCachePath.toString());
      return cachedInputStream;
    } catch (FileNotFoundException e) {
      this.log.info("CACHE MISS: " + resolvedCachePath.toString());
      // Cache miss, let's download from the origin
      FSDataInputStream originInputStream = downloadFromOrigin(resolvedOriginURI);

      // Let's persist to the cache
      Path folderInCacheForFile = resolvedCachePath.getParent();
      Path tempCacheFile = new Path(folderInCacheForFile, String.format(CACHE_TMP_FILE_TEMPLATE, RAND.nextInt()));
      FSDataOutputStream outStreamToTmpFile;
      try {
        this.cacheFS.mkdirs(folderInCacheForFile);
        outStreamToTmpFile = this.cacheFS.create(tempCacheFile, false);
      } catch (IOException e2) {
        // We failed to create the output stream, so just return the stream from the origin
        this.log.warn("Failed to persist file to cache, returning stream from origin: " +
            resolvedOriginURI.toString(), e2);
        return originInputStream;
      }

      // Copy from origin to tmp file in cache
      IOUtils.copy(originInputStream, outStreamToTmpFile);
      originInputStream.close();
      outStreamToTmpFile.close();

      try {
        // Rename the temporary file to the final file name
        this.cacheFS.rename(tempCacheFile, resolvedCachePath);
      } catch (FileAlreadyExistsException e2) {
        this.log.info("Another process already persisted this file: " + resolvedCachePath.toString());
        // Another process beat us to the race - no problem though, that means the file already exists so we
        // can just swallow this error and return the stream like usual!
      }

      // Return input stream from cache
      return this.cacheFS.open(resolvedCachePath, bufferSize);
    }
  }

  @Override
  public FSDataOutputStream create(final Path path, final FsPermission fsPermission,
      final boolean b, final int i, final short i1, final long l,
      final Progressable progressable)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream append(final Path path, final int i, final Progressable progressable)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rename(final Path path, final Path path1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean delete(final Path path, final boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileStatus[] listStatus(final Path path) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setWorkingDirectory(final Path path) {
  }

  @Override
  public Path getWorkingDirectory() {
    return WORKING_DIR;
  }

  @Override
  public boolean mkdirs(final Path path, final FsPermission fsPermission)
      throws IOException {
    return false;
  }

  @Override
  public FileStatus getFileStatus(final Path path) throws IOException {
    return new FileStatus(-1, false, 1, DEFAULT_BLOCK_SIZE, 0, path);
  }

  static class HttpDataInputStream extends FilterInputStream
      implements Seekable, PositionedReadable {

    HttpDataInputStream(final InputStream in) {
      super(in);
    }

    @Override
    public int read(final long position, final byte[] buffer, final int offset, final int length)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(final long position, final byte[] buffer, final int offset, final int length)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(final long position, final byte[] buffer) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seek(final long pos) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getPos() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean seekToNewSource(final long targetPos) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  FSDataInputStream downloadFromOrigin(final URI uri) throws IOException {
    URLConnection conn = uri.toURL().openConnection();
    InputStream in = conn.getInputStream();
    return new FSDataInputStream(new HttpDataInputStream(in));
  }
}
