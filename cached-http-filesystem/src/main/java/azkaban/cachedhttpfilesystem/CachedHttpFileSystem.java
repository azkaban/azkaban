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
import org.apache.http.protocol.HTTP;
import org.apache.log4j.Logger;

import static java.util.Objects.*;


/**
 * CachedHttpFileSystem behaves almost identically to Hadoop's native HttpFileSystem except it adds caching logic to
 * reduce load on the HTTP origin.
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
  public static final String CACHE_ROOT_URI = "cachedhttpfilesystem.cache_root_uri";

  private static final long DEFAULT_BLOCK_SIZE = 4096;
  private static final Path WORKING_DIR = new Path("/");
  private static final String CACHE_TMP_FILE_TEMPLATE = "tmp%d.tmp";
  private static final String HTTP_SCHEME = "http";
  private static final Random RAND = new Random();

  private static final Logger log = Logger.getLogger(CachedHttpFileSystem.class);

  private URI uri;
  private FileSystem cacheFS;
  private URI rootCachedURI;
  private URI rootOriginURI;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    String cacheRootUri = conf.get(CACHE_ROOT_URI);
    requireNonNull(cacheRootUri);

    this.uri = URI.create(addTrailingForwardSlash(name.toString()));

    this.rootCachedURI = URI.create(addTrailingForwardSlash(cacheRootUri));

    URIBuilder rootOriginURIBuilder = new URIBuilder(this.uri);
    rootOriginURIBuilder.setScheme(HTTP_SCHEME);
    try {
      this.rootOriginURI = rootOriginURIBuilder.build();
    } catch (URISyntaxException e) {
      // This should never happen - we are only changing the scheme on an already valid URI.
      throw new RuntimeException(e);
    }

    this.cacheFS = FileSystem.get(rootCachedURI, conf);
  }

  // If the base URIs don't have a trailing forward slash the resolving and relativization can get messed up.
  private static String addTrailingForwardSlash(String in) {
    return in.endsWith("/") ? in : in + "/";
  }

  @Override
  public String getScheme() {
    return "chttp";
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    URI relativeURI = this.uri.relativize(path.toUri());
    if (relativeURI.isAbsolute()) {
      throw new IOException("Path must be relative or have same prefix as root origin URI.");
    }

    URI resolvedOriginURI = this.rootOriginURI.resolve(relativeURI);
    Path resolvedCachePath = new Path(this.rootCachedURI.resolve(relativeURI));

    try {
      // Try to pull from cache
      return this.cacheFS.open(resolvedCachePath, bufferSize);
    } catch (FileNotFoundException e) {
      log.info("Cache miss, file not found: " + resolvedCachePath.toString());
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
        log.warn("Failed to persist file to cache, returning stream from origin: " +
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
        log.info("Another process already persisted this file: " + resolvedCachePath.toString());
        // Another process beat us to the race - no problem though, that means the file already exists so we
        // can just swallow this error and return the stream like usual!
      }

      // Return input stream from cache
      return this.cacheFS.open(resolvedCachePath, bufferSize);
    }
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission,
      boolean b, int i, short i1, long l,
      Progressable progressable)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rename(Path path, Path path1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean delete(Path path, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setWorkingDirectory(Path path) {
  }

  @Override
  public Path getWorkingDirectory() {
    return WORKING_DIR;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission)
      throws IOException {
    return false;
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    return new FileStatus(-1, false, 1, DEFAULT_BLOCK_SIZE, 0, path);
  }

  static class HttpDataInputStream extends FilterInputStream
      implements Seekable, PositionedReadable {

    HttpDataInputStream(InputStream in) {
      super(in);
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long pos) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getPos() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  FSDataInputStream downloadFromOrigin(URI uri) throws IOException {
    URLConnection conn = uri.toURL().openConnection();
    InputStream in = conn.getInputStream();
    return new FSDataInputStream(new HttpDataInputStream(in));
  }
}
