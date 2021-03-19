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

import static azkaban.HadoopModule.HADOOP_FS_AUTH;
import static azkaban.HadoopModule.HDFS_CACHED_HTTP_FS;
import static azkaban.HadoopModule.HADOOP_FILE_CONTEXT;
import static java.util.Objects.requireNonNull;

import azkaban.AzkabanCommonModuleConfig;
import azkaban.spi.Dependency;
import azkaban.spi.ProjectStorageMetadata;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.utils.StorageUtils;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.EnumSet;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class HdfsStorage implements Storage {

  private static final String TMP_PROJECT_UPLOAD_FILENAME = "upload.tmp";
  private static final Logger log = LoggerFactory.getLogger(HdfsStorage.class);

  // Size of the buffer used while transferring data upstream. Value is based on the default
  // value used in FileSystem.copyFromLocalFile().
  private static final int UPLOAD_BUFFER_SIZE_BYTES = 4096;

  private final AbstractHdfsAuth hdfsAuth;
  private final URI projectRootUri;
  private final URI dependencyRootUri;

  // We are using FileContext (instead of FileSystem) for Hdfs because putProject() does a rename()
  // with the Overwrite option. This rename() is a public method in FileContext but protected in
  // FileSystem. Also, FileSystem.rename() is deprecated.
  // Note that the original implementation here was using DistributedFileSystem (instead of
  // FileSystem) for which the rename(with overwrite) is public, but that ties this HdfsStorage
  // class to a concrete implementation of FileSystem, which made this very inflexible.
  // We should consider replacing all uses of the FileSystem with FileContext within azkaban to
  // maintain consistency in our code and also because FileContext is a modern alternative that
  // deprecates several methods of FileSystem.
  private final FileContext hdfsFileContext;
  private final FileSystem http;

  @Inject
  public HdfsStorage(final AzkabanCommonModuleConfig config,
      @Named(HADOOP_FS_AUTH) final AbstractHdfsAuth hdfsAuth,
      @Named(HADOOP_FILE_CONTEXT) final FileContext hdfsFileContext,
      @Named(HDFS_CACHED_HTTP_FS) @Nullable final FileSystem http) {
    this.hdfsAuth = requireNonNull(hdfsAuth);
    this.hdfsFileContext = requireNonNull(hdfsFileContext);
    this.http = http; // May be null if thin archives is not enabled

    this.projectRootUri = config.getHdfsProjectRootUri();
    this.dependencyRootUri = config.getOriginDependencyRootUri();
  }

  @Override
  public InputStream getProject(final String key) throws IOException {
    this.hdfsAuth.authorize();
    final Path projectFilePath = fullProjectPath(key);
    log.info("Opening for reading, project file " + projectFilePath);
    return this.hdfsFileContext.open(projectFilePath);
  }

  // IOUtils.closeStreams() is an alternative to this, but it doesn't log exceptions by default
  // and making it use the log4j Logger could be tricky.
  private static void closeStreamsQuietly(final Closeable... streams) {
    for (final Closeable stream : streams) {
      if (stream == null) {
        continue;
      }
      try {
        stream.close();
      } catch (final IOException e) {
        log.error("Exception while closing stream " + stream, e);
      }
    }
  }

  /**
   * Copy a file from the local file system to a location given by the file context. {@code
   * remotePath and remoteFileContext} will typically point to an external location but don't have
   * to. If {@code remotePath} already exists, it will be overwritten.
   * <p>
   * Note that FileSystem includes a convenient {@code copyFromLocalFile} method that can be
   * directly used in place of this. FileContext does not appear to have a similar functionality
   * built-in, requiring this implementation.
   *
   * @param localPath         location on local disk
   * @param remotePath        location where the file will be copied to
   * @param remoteFileContext file context for the remote file path
   * @throws IOException
   */
  @VisibleForTesting
  static void uploadLocalFile(final Path localPath, final Path remotePath,
      final FileContext remoteFileContext) throws IOException {
    FSDataInputStream localStream = null;
    FSDataOutputStream remoteStream = null;

    final String actionString = String.format("upload of local file %s to remote file %s",
        localPath, remotePath);
    log.debug("Starting " + actionString);

    try {
      final FileContext localFileContext = FileContext.getLocalFSFileContext();
      localStream = localFileContext.open(localPath);
      remoteStream = remoteFileContext.create(remotePath,
          EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));
      IOUtils.copyBytes(localStream, remoteStream, UPLOAD_BUFFER_SIZE_BYTES, true);
    } catch (final IOException e) {
      log.error("Error during " + actionString, e);
      throw e;
    } finally {
      closeStreamsQuietly(localStream, remoteStream);
      log.debug("Ending " + actionString);
      // FileContexts (localFileContext in this case) don't need to be 'closed'
    }
  }

  @Override
  public String putProject(final ProjectStorageMetadata metadata, final File localFile) {
    this.hdfsAuth.authorize();
    final Path projectsPath = new Path(this.projectRootUri.getPath(),
        String.valueOf(metadata.getProjectId()));
    try {
      this.hdfsFileContext.mkdir(projectsPath, FsPermission.getDefault(), true);
      log.info("Created project dir: " + projectsPath);
      final Path targetPath = new Path(projectsPath,
          StorageUtils.getTargetProjectFilename(metadata.getProjectId(), metadata.getHash()));
      final Path tmpPath = new Path(projectsPath, TMP_PROJECT_UPLOAD_FILENAME);

      final Path localFilePath = new Path(localFile.getPath())
          .makeQualified(FsConstants.LOCAL_FS_URI, null);
      log.info("Scheme qualified path of the local zip file is " + localFilePath);

      // Copy file to HDFS
      log.info(String.format("Creating project artifact: meta: %s path: %s", metadata, targetPath));
      uploadLocalFile(localFilePath, tmpPath, this.hdfsFileContext);

      // Rename the tmp file to the final file and overwrite the final file if it already exists
      // (i.e. if the hash is the same).
      // Note that the base implementation of rename with overwrite in FileContext does not
      // guarantee atomicity, i.e it's possible the existing file could be deleted without the
      // subsequent rename completing. This behavior could be problematic for Azkaban as this corner
      // case may arise when a user re-uploads a zip without any modification, in which case the
      // hash (thus the filename) will match that of the already existing zip file.
      // The default/fallback implementation configured in HadoopModule uses the class
      // org.apache.hadoop.fs.Hdfs for this, which does provide atomicity. Please check, in case
      // the implementation is picked from  core-site.xml that it resolves to a class with atomic
      // rename.
      this.hdfsFileContext.rename(tmpPath, targetPath, Options.Rename.OVERWRITE);

      return getRelativeProjectPath(targetPath);
    } catch (final IOException e) {
      log.error("error in putProject(): Metadata: " + metadata);
      throw new StorageException(e);
    }
  }

  @Override
  public InputStream getDependency(final Dependency dep) throws IOException {
    if (!dependencyFetchingEnabled()) {
      throw new UnsupportedOperationException("Dependency fetching is not enabled.");
    }

    // CachedHttpFileSystem will cache in HDFS, so it needs to be authenticated to access HDFS!
    this.hdfsAuth.authorize();
    return this.http.open(resolveAbsoluteDependencyURI(dep));
  }

  @Override
  public boolean dependencyFetchingEnabled() {
    return this.http != null;
  }

  @Override
  public boolean deleteProject(final String key) {
    this.hdfsAuth.authorize();
    final Path path = fullProjectPath(key);
    try {
      return this.hdfsFileContext.delete(path, false);
    } catch (final IOException e) {
      log.error("HDFS project file delete failed on " + path, e);
      return false;
    }
  }

  private Path fullProjectPath(final String key) {
    return new Path(this.projectRootUri.toString(), key);
  }

  private Path resolveAbsoluteDependencyURI(final Dependency dep) {
    return new Path(this.dependencyRootUri.toString(), StorageUtils.getTargetDependencyPath(dep));
  }

  @Override
  public String getDependencyRootPath() {
    return this.dependencyRootUri != null ? this.dependencyRootUri.toString() : null;
  }

  private String getRelativeProjectPath(final Path targetPath) {
    return URI.create(this.projectRootUri.getPath()).relativize(targetPath.toUri()).getPath();
  }
}
