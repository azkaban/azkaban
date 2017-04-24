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

import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.spi.StorageMetadata;
import azkaban.utils.FileIOUtils;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import static com.google.common.base.Preconditions.*;


public class LocalStorage implements Storage {
  private static final Logger log = Logger.getLogger(LocalStorage.class);

  final File baseDirectory;

  public LocalStorage(File baseDirectory) {
    this.baseDirectory = validateBaseDirectory(createIfDoesNotExist(baseDirectory));
  }

  @Override
  public InputStream get(URI key) {
    try {
      return new FileInputStream(getFile(key));
    } catch (FileNotFoundException e) {
      return null;
    }
  }

  private File getFile(URI key) {
    return new File(baseDirectory, key.getPath());
  }

  @Override
  public URI put(StorageMetadata metadata, File localFile) {

    final File projectDir = new File(baseDirectory, String.valueOf(metadata.getProjectId()));
    if (projectDir.mkdir()) {
      log.info("Created project dir: " + projectDir.getAbsolutePath());
    }

    final File targetFile = new File(projectDir,
        metadata.getVersion() + "." + Files.getFileExtension(localFile.getName()));

    if (targetFile.exists()) {
      throw new StorageException(String.format(
          "Error in LocalStorage. Target file already exists. targetFile: %s, Metadata: %s",
          targetFile, metadata));
    }
    try {
      FileUtils.copyFile(localFile, targetFile);
    } catch (IOException e) {
      log.error("LocalStorage error in put(): Metadata: " + metadata);
      throw new StorageException(e);
    }
    return createRelativeURI(targetFile);
  }

  private URI createRelativeURI(File targetFile) {
    return baseDirectory.toURI().relativize(targetFile.toURI());
  }

  @Override
  public boolean delete(URI key) {
    throw new UnsupportedOperationException("delete has not been implemented.");
  }

  private static File createIfDoesNotExist(File baseDirectory) {
    if(!baseDirectory.exists()) {
      baseDirectory.mkdir();
      log.info("Creating dir: " + baseDirectory.getAbsolutePath());
    }
    return baseDirectory;
  }

  private static File validateBaseDirectory(File baseDirectory) {
    checkArgument(baseDirectory.isDirectory());
    if (!FileIOUtils.isDirWritable(baseDirectory)) {
      throw new IllegalArgumentException("Directory not writable: " + baseDirectory);
    }
    return baseDirectory;
  }
}
