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

import azkaban.spi.KeyDoesNotExistException;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import static com.google.common.base.Preconditions.*;
import static java.util.Objects.*;


public class LocalStorage implements Storage {
  private static final Logger log = Logger.getLogger(LocalStorage.class);

  public static final String PROJECT = "project";
  public static final String VERSION = "version";
  public static final String EXTENSION = "extension";

  final File baseDirectory;

  public LocalStorage(File baseDirectory) {
    this.baseDirectory = validateBaseDirectory(baseDirectory);
  }

  @Override
  public InputStream get(URI key) throws KeyDoesNotExistException {
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
  public URI put(Properties metadata, InputStream is) {
    checkArgument(metadata.containsKey(PROJECT), "Incorrect metadata. project not found");
    checkArgument(metadata.containsKey(VERSION), "Incorrect metadata. version not found");
    checkArgument(metadata.containsKey(EXTENSION), "Incorrect metadata. extension not found");

    final String projectId = requireNonNull(metadata.getProperty(PROJECT), "project is null");
    final String version = requireNonNull(metadata.getProperty(VERSION), "version is null");

    final File projectDir = new File(baseDirectory, projectId);
    if (projectDir.mkdir()) {
      log.info("Created project dir: " + projectDir.getAbsolutePath());
    }

    final File targetFile = new File(projectDir, version + "." + metadata.getProperty(EXTENSION));

    if (targetFile.exists()) {
      throw new StorageException("Error in LocalStorage. Target file already exists. targetFile: " + targetFile);
    }
    try {
      FileUtils.copyInputStreamToFile(is, targetFile);
    } catch (IOException e) {
      throw new StorageException(e);
    }
    return createRelativeURI(targetFile);
  }

  private URI createRelativeURI(File targetFile) {
    return baseDirectory.toURI().relativize(targetFile.toURI());
  }

  @Override
  public void delete(URI key) throws KeyDoesNotExistException {
    throw new UnsupportedOperationException("delete has not been implemented.");
  }

  private static File validateBaseDirectory(File baseDirectory) {
    if(!baseDirectory.exists()) {
      baseDirectory.mkdir();
      log.info("Creating dir: " + baseDirectory.getAbsolutePath());
    }
    checkArgument(baseDirectory.isDirectory());
    try {
      File testFile = new File(baseDirectory, "_tmp");
      /*
       * Create and delete a dummy file in order to check file permissions. Maybe
       * there is a safer way for this check.
       */
      testFile.createNewFile();
      testFile.delete();
    } catch (IOException e) {
      throw new IllegalArgumentException("Directory not writable: " + baseDirectory);
    }
    return baseDirectory;
  }
}
