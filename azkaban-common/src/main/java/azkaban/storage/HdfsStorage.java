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

import static azkaban.utils.StorageUtils.*;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import azkaban.AzkabanCommonModuleConfig;
import azkaban.spi.FileIOStatus;
import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.spi.ProjectStorageMetadata;
import azkaban.utils.Props;
import java.io.FileNotFoundException;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;


@Singleton
public class HdfsStorage implements Storage {
  private static final Logger log = Logger.getLogger(HdfsStorage.class);
  private static final String HDFS_SCHEME = "hdfs";
  public static final String DEPENDENCY_FOLDER = "startup_dependencies";

  private final HdfsAuth hdfsAuth;
  private final URI rootUri;
  private final FileSystem hdfs;
  private final DistributedFileSystem dfs;

  private final Path dependencyPath;

  @Inject
  public HdfsStorage(final HdfsAuth hdfsAuth, final FileSystem hdfs, final DistributedFileSystem dfs,
      final AzkabanCommonModuleConfig config, final Props props) throws IOException {
    this.hdfsAuth = requireNonNull(hdfsAuth);
    this.hdfs = requireNonNull(hdfs);
    this.dfs = requireNonNull(dfs);

    this.rootUri = config.getHdfsRootUri();
    requireNonNull(this.rootUri.getAuthority(), "URI must have host:port mentioned.");
    checkArgument(HDFS_SCHEME.equals(this.rootUri.getScheme()));

    this.dfs.initialize(this.rootUri, this.hdfs.getConf());

    this.dependencyPath = new Path(this.rootUri.getPath(), DEPENDENCY_FOLDER);
    if (this.hdfs.mkdirs(this.dependencyPath)) {
      log.info("Created dir for jar dependencies: " + this.dependencyPath);
    }

    props.put(DEPENDENCY_STORAGE_PATH_PREFIX_PROP, this.rootUri.toASCIIString() + "/" + DEPENDENCY_FOLDER);
  }

  @Override
  public InputStream getProject(final String key) throws IOException {
    this.hdfsAuth.authorize();
    return this.hdfs.open(fullPath(key));
  }

  @Override
  public String putProject(final ProjectStorageMetadata metadata, final File localFile) {
    this.hdfsAuth.authorize();
    final Path projectsPath = new Path(this.rootUri.getPath(),
        String.valueOf(metadata.getProjectId()));
    try {
      if (this.hdfs.mkdirs(projectsPath)) {
        log.info("Created project dir: " + projectsPath);
      }
      final Path targetPath = new Path(projectsPath,
          getTargetProjectFilename(metadata.getProjectId(), metadata.getHash()));
      if (this.hdfs.exists(targetPath)) {
        log.info(
            String.format("Duplicate Found: meta: %s path: %s", metadata, targetPath));
        return getRelativePath(targetPath);
      }

      // Copy file to HDFS
      log.info(String.format("Creating project artifact: meta: %s path: %s", metadata, targetPath));
      this.hdfs.copyFromLocalFile(new Path(localFile.getAbsolutePath()), targetPath);
      return getRelativePath(targetPath);
    } catch (final IOException e) {
      log.error("error in put(): Metadata: " + metadata);
      throw new StorageException(e);
    }
  }

  @Override
  public FileIOStatus putDependency(final DependencyFile f) throws IOException {
    FileIOStatus status = dependencyStatus(f);
    if (status == FileIOStatus.NON_EXISTANT) {
      try {
        writeDependency(f);
        status = FileIOStatus.CLOSED;
      } catch (FileAlreadyExistsException e) {
        // Looks like another process beat us to the race. It started writing the file before we could.
        // It's possible that the process completed writing the file, but it's also possible that the file is
        // still being written to. We will assume the worst case (the file is still being written to) and
        // return a status of OPEN so as not to persist this entry in the DB. Next time a project is uploaded
        // that depends on this dependency, it will be identified as CLOSED on storage and then persisted in DB.
        // So essentially, we're just deferring caching the results of validation for this dependency until next
        // project upload.
        status = FileIOStatus.OPEN;
      }
    }
    return status;
  }

  private void writeDependency(final DependencyFile f) throws FileAlreadyExistsException {
    this.hdfsAuth.authorize();
    try {
      // Copy file to HDFS
      final Path targetPath = getDependencyPath(f);
      log.info(String.format("Uploading dependency to HDFS: %s -> %s", f.getFileName(), targetPath));
      this.hdfs.mkdirs(targetPath.getParent());
      this.hdfs.copyFromLocalFile(new Path(f.getFile().getAbsolutePath()), targetPath);
    } catch (final org.apache.hadoop.fs.FileAlreadyExistsException e) {
      // Either the file already exists, OR another web server process is uploading it
      log.info("Upload stopped. Dependency already exists in HDFS: " + f.getFileName());
      throw new FileAlreadyExistsException(e.getMessage());
    } catch (final IOException e) {
      log.error("Error uploading dependency to HDFS: " + f.getFileName());
      throw new StorageException(e);
    }
  }

  @Override
  public InputStream getDependency(final Dependency dep) throws IOException {
    this.hdfsAuth.authorize();
    return this.hdfs.open(getDependencyPath(dep));
  }

  @Override
  public FileIOStatus dependencyStatus(final Dependency dep) throws IOException {
    this.hdfsAuth.authorize();
    try {
      return dfs.isFileClosed(getDependencyPath(dep)) ? FileIOStatus.CLOSED : FileIOStatus.OPEN;
    } catch (final FileNotFoundException e) {
      return FileIOStatus.NON_EXISTANT;
    }
  }

  private Path getDependencyPath(Dependency dep) {
    return new Path(this.dependencyPath, getTargetDependencyPath(dep));
  }

  private String getRelativePath(final Path targetPath) {
    return URI.create(this.rootUri.getPath()).relativize(targetPath.toUri()).getPath();
  }

  @Override
  public boolean delete(final String key) {
    this.hdfsAuth.authorize();
    final Path path = fullPath(key);
    try {
      return this.hdfs.delete(path, false);
    } catch (final IOException e) {
      log.error("HDFS delete failed on " + path, e);
      return false;
    }
  }

  private Path fullPath(final String key) {
    return new Path(this.rootUri.toString(), key);
  }
}
