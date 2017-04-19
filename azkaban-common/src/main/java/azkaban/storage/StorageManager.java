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

import azkaban.project.Project;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.spi.StorageMetadata;
import com.google.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import org.apache.log4j.Logger;


/**
 * StorageManager manages and coordinates all interactions with the Storage layer. This also includes bookkeeping
 * like updating DB with the new versionm, etc
 */
public class StorageManager {
  private static final Logger log = Logger.getLogger(StorageManager.class);

  private final Storage storage;

  @Inject
  public StorageManager(Storage storage) {
    this.storage = storage;
  }

  /**
   * API to a project file into Azkaban Storage
   *
   * @param project           project ID
   * @param fileExtension     extension of the file
   * @param filename          name of the file
   * @param localFile         local file
   * @param uploader          the user who uploaded
   */
  public void uploadProject(
      Project project,
      String fileExtension,
      String filename,
      File localFile,
      String uploader) {
    final StorageMetadata metadata = new StorageMetadata(
        String.valueOf(project.getId()),
        String.valueOf(getLatestVersion(project)),
        fileExtension
    );
    log.info(String.format(
        "Uploading project. Uploader: %s, Metadata:%s, filename: %s[%d bytes]",
        uploader, metadata, filename, localFile.length()
    ));
    try {
      uploadProject(metadata, new FileInputStream(localFile));
    } catch (FileNotFoundException e) {
      throw new StorageException(e);
    }
  }

  private int getLatestVersion(Project project) {
    // TODO Implement
    return -1;
  }

  public void uploadProject(StorageMetadata metadata, InputStream is) {
    // TODO Implement
    URI key = storage.put(metadata, is);
  }
}
