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
import com.google.common.io.Files;
import com.google.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class LocalStorage implements Storage {

  private static final Logger log = Logger.getLogger(LocalStorage.class);

  final File rootDirectory;

  @Inject
  public LocalStorage(AzkabanCommonModuleConfig config) {
    this.rootDirectory = validateRootDirectory(
        createIfDoesNotExist(config.getLocalStorageBaseDirPath()));
  }

  /**
   * @param key Relative path of the file from the baseDirectory
   */
  @Override
  public InputStream get(String key) throws IOException {
    return new FileInputStream(new File(rootDirectory, key));
  }

  @Override
  public String put(StorageMetadata metadata, File localFile) {
    final File projectDir = new File(rootDirectory, String.valueOf(metadata.getProjectId()));
    if (projectDir.mkdir()) {
      log.info("Created project dir: " + projectDir.getAbsolutePath());
    }

    final File targetFile = new File(projectDir, String.format("%s-%s.%s",
        String.valueOf(metadata.getProjectId()),
        new String(metadata.getHash()),
        Files.getFileExtension(localFile.getName())));

    if (targetFile.exists()) {
      log.info(String.format("Duplicate found: meta: %s, targetFile: %s, ", metadata,
          targetFile.getAbsolutePath()));
      return getRelativePath(targetFile);
    }

    // Copy file to storage dir
    try {
      FileUtils.copyFile(localFile, targetFile);
    } catch (IOException e) {
      log.error("LocalStorage error in put(): meta: " + metadata);
      throw new StorageException(e);
    }
    return getRelativePath(targetFile);
  }

  private String getRelativePath(File targetFile) {
    return rootDirectory.toURI().relativize(targetFile.toURI()).getPath();
  }

  @Override
  public boolean delete(String key) {
    throw new UnsupportedOperationException("delete has not been implemented.");
  }

  private static File createIfDoesNotExist(String baseDirectoryPath) {
    final File baseDirectory = new File(baseDirectoryPath);
    if (!baseDirectory.exists()) {
      baseDirectory.mkdir();
      log.info("Creating dir: " + baseDirectory.getAbsolutePath());
    }
    return baseDirectory;
  }

  private static File validateRootDirectory(File baseDirectory) {
    checkArgument(baseDirectory.isDirectory());
    if (!FileIOUtils.isDirWritable(baseDirectory)) {
      throw new IllegalArgumentException("Directory not writable: " + baseDirectory);
    }
    return baseDirectory;
  }
}
