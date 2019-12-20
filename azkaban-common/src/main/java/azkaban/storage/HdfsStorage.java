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

import static java.util.Objects.requireNonNull;

import azkaban.AzkabanCommonModuleConfig;
import azkaban.utils.HashUtils;
import azkaban.utils.StorageUtils;
import azkaban.spi.Dependency;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.spi.ProjectStorageMetadata;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;


@Singleton
public class HdfsStorage implements Storage {
  private static final String TMP_PROJECT_UPLOAD_FILENAME = "upload.tmp";
  private static final Logger log = Logger.getLogger(HdfsStorage.class);

  private final HdfsAuth hdfsAuth;
  private final URI projectRootUri;
  private final URI dependencyRootUri;
  private final DistributedFileSystem hdfs;
  private final FileSystem http;

  @Inject
  public HdfsStorage(final AzkabanCommonModuleConfig config, final HdfsAuth hdfsAuth, @Named("hdfsFS") final FileSystem hdfs,
      @Named("hdfs_cached_httpFS") @Nullable final FileSystem http) {
    this.hdfsAuth = requireNonNull(hdfsAuth);
    // Usually we can just interact with this object as a FileSystem however putProject() uses
    // the .rename() method with the OVERRIDE option which is protected in FileSystem but
    // public in DistributedFileSystem so we need to cast it here.
    this.hdfs = (DistributedFileSystem) requireNonNull(hdfs);
    this.http = http; // May be null if thin archives is not enabled

    this.projectRootUri = config.getHdfsProjectRootUri();
    this.dependencyRootUri = config.getOriginDependencyRootUri();
  }

  @Override
  public InputStream getProject(final String key) throws IOException {
    this.hdfsAuth.authorize();
    return this.hdfs.open(fullProjectPath(key));
  }

  @Override
  public String putProject(final ProjectStorageMetadata metadata, final File localFile) {
    this.hdfsAuth.authorize();
    final Path projectsPath = new Path(this.projectRootUri.getPath(),
        String.valueOf(metadata.getProjectId()));
    try {
      if (this.hdfs.mkdirs(projectsPath)) {
        log.info("Created project dir: " + projectsPath);
      }
      final Path targetPath = new Path(projectsPath,
          StorageUtils.getTargetProjectFilename(metadata.getProjectId(), metadata.getHash()));
      final Path tmpPath = new Path(projectsPath, TMP_PROJECT_UPLOAD_FILENAME);

      // Copy file to HDFS
      log.info(String.format("Creating project artifact: meta: %s path: %s", metadata, targetPath));
      this.hdfs.copyFromLocalFile(false, true, new Path(localFile.getAbsolutePath()), tmpPath);

      // Rename the tmp file to the final file and overwrite the final file if it already exists
      // (i.e. if the hash is the same).
      this.hdfs.rename(tmpPath, targetPath, Options.Rename.OVERWRITE);

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
      return this.hdfs.delete(path, false);
    } catch (final IOException e) {
      log.error("HDFS project file delete failed on " + path, e);
      return false;
    }
  }

  private Path fullProjectPath(final String key) {
    return new Path(this.projectRootUri.toString(), key);
  }

  private Path resolveAbsoluteDependencyURI(Dependency dep) {
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
