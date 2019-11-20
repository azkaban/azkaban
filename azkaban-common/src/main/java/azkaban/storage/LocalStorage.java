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

import azkaban.AzkabanCommonModuleConfig;
import azkaban.spi.Dependency;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.spi.ProjectStorageMetadata;
import azkaban.utils.FileIOUtils;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

@Singleton
public class LocalStorage implements Storage {
  private static final Logger log = Logger.getLogger(LocalStorage.class);

  final File rootDirectory;

  @Inject
  public LocalStorage(final AzkabanCommonModuleConfig config) {
    this.rootDirectory = validateDirectory(createIfDoesNotExist(config.getLocalStorageBaseDirPath()));
  }

  private static File createIfDoesNotExist(final File baseDirectory) {
    if (!baseDirectory.exists()) {
      baseDirectory.mkdirs();
      log.info("Creating dir: " + baseDirectory.getAbsolutePath());
    }
    return baseDirectory;
  }

  private static File createIfDoesNotExist(final String baseDirectoryPath) {
    return createIfDoesNotExist(new File(baseDirectoryPath));
  }

  private static File validateDirectory(final File baseDirectory) {
    checkArgument(baseDirectory.isDirectory());
    if (!FileIOUtils.isDirWritable(baseDirectory)) {
      throw new IllegalArgumentException("Directory not writable: " + baseDirectory);
    }
    return baseDirectory;
  }

  private File getFileInRoot(final String key) {
    return new File(this.rootDirectory, key);
  }

  /**
   * @param key Relative path of the file from the baseDirectory
   */
  @Override
  public InputStream getProject(final String key) throws IOException {
    return new FileInputStream(getFileInRoot(key));
  }

  @Override
  public String putProject(final ProjectStorageMetadata metadata, final File localFile) {
    final File projectDir = new File(this.rootDirectory, String.valueOf(metadata.getProjectId()));
    if (projectDir.mkdir()) {
      log.info("Created project dir: " + projectDir.getAbsolutePath());
    }
    final File targetFile = new File(projectDir,
        getTargetProjectFilename(metadata.getProjectId(), metadata.getHash()));

    if (targetFile.exists()) {
      log.info(String.format("Duplicate found: meta: %s, targetFile: %s, ", metadata,
          targetFile.getAbsolutePath()));
      return getRelativePath(targetFile);
    }

    // Copy file to storage dir
    try {
      FileUtils.copyFile(localFile, targetFile);
    } catch (final IOException e) {
      log.error("LocalStorage error in putProject(): meta: " + metadata);
      throw new StorageException(e);
    }
    return getRelativePath(targetFile);
  }

  // LocalStorage does not support dependency fetching and thus does not support thin archives.
  // Use LocalHadoopStorage or HdfsStorage instead for thin archive support.
  @Override
  public InputStream getDependency(final Dependency dep) throws IOException {
    throw new UnsupportedOperationException("Dependency fetching is not supported with LocalStorage.");
  }

  @Override
  public boolean dependencyFetchingEnabled() {
    return false;
  }

  @Override
  public String getDependencyRootPath() {
    return null;
  }

  @Override
  public boolean deleteProject(final String key) {
    final File file = getFileInRoot(key);
    final boolean result = file.exists() && file.delete();
    if (result) {
      log.warn("Deleted project file: " + file.getAbsolutePath());
    } else {
      log.warn("Unable to delete project file: " + file.getAbsolutePath());
    }
    return result;
  }

  private String getRelativePath(final File targetFile) {
    return this.rootDirectory.toURI().relativize(targetFile.toURI()).getPath();
  }
}
