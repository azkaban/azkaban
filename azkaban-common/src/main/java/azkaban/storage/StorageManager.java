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
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.spi.StorageMetadata;
import azkaban.user.User;
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
   * TODO clean up interface
   *
   * @param project           project
   * @param version           The new version to be uploaded
   * @param localFile         local file
   * @param uploader          the user who uploaded
   */
  public void uploadProject(
      Project project,
      int version,
      File localFile,
      User uploader) {
    final StorageMetadata metadata = new StorageMetadata(
        project.getId(),
        version,
        uploader.getUserId()
    );
    log.info(String.format("Adding archive to storage. Meta:%s File: %s[%d bytes]",
        metadata, localFile.getName(), localFile.length()));
    storage.put(metadata, localFile);
  }
}
