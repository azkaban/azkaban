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
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.spi.StorageMetadata;
import com.google.common.io.Files;
import com.google.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import static com.google.common.base.Preconditions.*;
import static java.util.Objects.*;


public class HdfsStorage implements Storage {
  private static final Logger log = Logger.getLogger(HdfsStorage.class);
  private static final String HDFS_SCHEME = "hdfs";

  private final URI rootUri;
  private final FileSystem hdfs;

  @Inject
  public HdfsStorage(FileSystem hdfs, AzkabanCommonModuleConfig config) {
    this.rootUri = config.getHdfsRootUri();
    requireNonNull(rootUri.getAuthority(), "URI must have host:port mentioned.");
    checkArgument(HDFS_SCHEME.equals(rootUri.getScheme()));
    this.hdfs = hdfs;
  }

  @Override
  public InputStream get(String key) throws IOException {
    return hdfs.open(new Path(rootUri.toString(), key));
  }

  @Override
  public String put(StorageMetadata metadata, File localFile) {
    final Path projectsPath = new Path(rootUri.getPath(), String.valueOf(metadata.getProjectId()));
    try {
      if (hdfs.mkdirs(projectsPath)) {
        log.info("Created project dir: " + projectsPath);
      }
      final Path targetPath = createTargetPath(metadata, localFile, projectsPath);
      if ( hdfs.exists( targetPath )) {
        throw new StorageException(String.format(
            "Error: Target file already exists. targetFile: %s, Metadata: %s",
            targetPath, metadata));
      }
      hdfs.copyFromLocalFile(new Path(localFile.getAbsolutePath()), targetPath);
      return URI.create(rootUri.getPath()).relativize(targetPath.toUri()).getPath();
    } catch (IOException e) {
      log.error("error in put(): Metadata: " + metadata);
      throw new StorageException(e);
    }
  }

  private Path createTargetPath(StorageMetadata metadata, File localFile, Path projectsPath) {
    return new Path(projectsPath, String.format("%s-%s.%s",
        String.valueOf(metadata.getProjectId()),
        new String(metadata.getHash()),
        Files.getFileExtension(localFile.getName())
    ));
  }

  @Override
  public boolean delete(String key) {
    throw new UnsupportedOperationException("Method not implemented");
  }
}
