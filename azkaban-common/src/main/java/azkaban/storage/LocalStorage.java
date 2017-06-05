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

import azkaban.AzkabanCommonModuleConfig;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.spi.StorageMetadata;
import azkaban.utils.FileIOUtils;
import com.google.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class LocalStorage implements Storage {

  private static final Logger log = Logger.getLogger(LocalStorage.class);

  final File rootDirectory;

  @Inject
  public LocalStorage(final AzkabanCommonModuleConfig config) {
    this.rootDirectory = validateRootDirectory(
        createIfDoesNotExist(config.getLocalStorageBaseDirPath()));
  }

  private static File createIfDoesNotExist(final String baseDirectoryPath) {
    final File baseDirectory = new File(baseDirectoryPath);
    if (!baseDirectory.exists()) {
      baseDirectory.mkdir();
      log.info("Creating dir: " + baseDirectory.getAbsolutePath());
    }
    return baseDirectory;
  }

  private static File validateRootDirectory(final File baseDirectory) {
    checkArgument(baseDirectory.isDirectory());
    if (!FileIOUtils.isDirWritable(baseDirectory)) {
      throw new IllegalArgumentException("Directory not writable: " + baseDirectory);
    }
    return baseDirectory;
  }

  /**
   * @param key Relative path of the file from the baseDirectory
   */
  @Override
  public InputStream get(final String key) throws IOException {
    return new FileInputStream(new File(this.rootDirectory, key));
  }

  @Override
  public String put(final StorageMetadata metadata, final File localFile) {
    final File projectDir = new File(this.rootDirectory, String.valueOf(metadata.getProjectId()));
    if (projectDir.mkdir()) {
      log.info("Created project dir: " + projectDir.getAbsolutePath());
    }

    final File targetFile = new File(projectDir, String.format("%s-%s.zip",
        String.valueOf(metadata.getProjectId()),
        new String(metadata.getHash(), StandardCharsets.UTF_8)));

    if (targetFile.exists()) {
      log.info(String.format("Duplicate found: meta: %s, targetFile: %s, ", metadata,
          targetFile.getAbsolutePath()));
      return getRelativePath(targetFile);
    }

    // Copy file to storage dir
    try {
      FileUtils.copyFile(localFile, targetFile);
    } catch (final IOException e) {
      log.error("LocalStorage error in put(): meta: " + metadata);
      throw new StorageException(e);
    }
    return getRelativePath(targetFile);
  }

  private String getRelativePath(final File targetFile) {
    return this.rootDirectory.toURI().relativize(targetFile.toURI()).getPath();
  }

  @Override
  public boolean delete(final String key) {
    throw new UnsupportedOperationException("delete has not been implemented.");
  }
}
