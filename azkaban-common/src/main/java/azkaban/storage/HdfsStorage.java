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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import azkaban.AzkabanCommonModuleConfig;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.spi.StorageMetadata;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class HdfsStorage implements Storage {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsStorage.class);
  private static final String HDFS_SCHEME = "hdfs";

  private final HdfsAuth hdfsAuth;
  private final URI rootUri;
  private final FileSystem hdfs;

  @Inject
  public HdfsStorage(final HdfsAuth hdfsAuth, final FileSystem hdfs,
      final AzkabanCommonModuleConfig config) {
    this.hdfsAuth = requireNonNull(hdfsAuth);
    this.hdfs = requireNonNull(hdfs);

    this.rootUri = config.getHdfsRootUri();
    requireNonNull(this.rootUri.getAuthority(), "URI must have host:port mentioned.");
    checkArgument(HDFS_SCHEME.equals(this.rootUri.getScheme()));
  }

  @Override
  public InputStream get(final String key) throws IOException {
    this.hdfsAuth.authorize();
    return this.hdfs.open(fullPath(key));
  }

  @Override
  public String put(final StorageMetadata metadata, final File localFile) {
    this.hdfsAuth.authorize();
    final Path projectsPath = new Path(this.rootUri.getPath(),
        String.valueOf(metadata.getProjectId()));
    try {
      if (this.hdfs.mkdirs(projectsPath)) {
        LOG.info("Created project dir: " + projectsPath);
      }
      final Path targetPath = createTargetPath(metadata, projectsPath);
      if (this.hdfs.exists(targetPath)) {
        LOG.info(String.format("Duplicate Found: meta: %s path: %s", metadata, targetPath));
        return getRelativePath(targetPath);
      }

      // Copy file to HDFS
      LOG.info(String.format("Creating project artifact: meta: %s path: %s", metadata, targetPath));
      this.hdfs.copyFromLocalFile(new Path(localFile.getAbsolutePath()), targetPath);
      return getRelativePath(targetPath);
    } catch (final IOException e) {
      LOG.error("error in put(): Metadata: " + metadata);
      throw new StorageException(e);
    }
  }

  private String getRelativePath(final Path targetPath) {
    return URI.create(this.rootUri.getPath()).relativize(targetPath.toUri()).getPath();
  }

  private Path createTargetPath(final StorageMetadata metadata, final Path projectsPath) {
    return new Path(projectsPath, String.format("%s-%s.zip",
        String.valueOf(metadata.getProjectId()),
        new String(Hex.encodeHex(metadata.getHash()))
    ));
  }

  @Override
  public boolean delete(final String key) {
    this.hdfsAuth.authorize();
    final Path path = fullPath(key);
    try {
      return this.hdfs.delete(path, false);
    } catch (final IOException e) {
      LOG.error("HDFS delete failed on " + path, e);
      return false;
    }
  }

  private Path fullPath(final String key) {
    return new Path(this.rootUri.toString(), key);
  }
}
