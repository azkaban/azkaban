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
import azkaban.project.ProjectFileHandler;
import azkaban.project.ProjectLoader;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.spi.StorageMetadata;
import azkaban.user.User;
import azkaban.utils.Md5Hasher;
import azkaban.utils.Props;
import com.google.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import static com.google.common.base.Preconditions.*;
import static java.util.Objects.*;


/**
 * StorageManager manages and coordinates all interactions with the Storage layer. This also includes bookkeeping
 * like updating DB with the new versionm, etc
 */
public class StorageManager {
  private static final Logger log = Logger.getLogger(StorageManager.class);

  private final Storage storage;
  private final ProjectLoader projectLoader;
  private final File tempDir;

  @Inject
  public StorageManager(Props props, Storage storage, ProjectLoader projectLoader) {
    this.tempDir = new File(props.getString("project.temp.dir", "temp"));
    this.storage = requireNonNull(storage);
    this.projectLoader = requireNonNull(projectLoader);

    prepareTempDir();
  }

  private void prepareTempDir() {
    if (!tempDir.exists()) {
      tempDir.mkdirs();
    }
    checkArgument(tempDir.isDirectory());
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
    byte[] md5 = null;
    if (!(storage instanceof DatabaseStorage)) {
      md5 = computeHash(localFile);
    }
    final StorageMetadata metadata = new StorageMetadata(
        project.getId(),
        version,
        uploader.getUserId(),
        md5);
    log.info(String.format("Adding archive to storage. Meta:%s File: %s[%d bytes]",
        metadata, localFile.getName(), localFile.length()));

    /* upload to storage */
    final String resourceId = storage.put(metadata, localFile);

    /* Add metadata to db */
    // TODO spyne: remove hack. Database storage should go through the same flow
    if (!(storage instanceof DatabaseStorage)) {
      projectLoader.addProjectVersion(
          project.getId(),
          version,
          localFile,
          uploader.getUserId(),
          requireNonNull(md5),
          requireNonNull(resourceId)
      );
      log.info(String.format("Added project metadata to DB. Meta:%s File: %s[%d bytes] URI: %s",
          metadata, localFile.getName(), localFile.length(), resourceId));
    }
  }

  private byte[] computeHash(File localFile) {
    final byte[] md5;
    try {
      md5 = Md5Hasher.md5Hash(localFile);
    } catch (IOException e) {
      throw new StorageException(e);
    }
    return md5;
  }

  /**
   * Fetch project file from storage.
   *
   * @param projectId required project ID
   * @param version version to be fetched
   * @return Handler object containing hooks to fetched project file
   */
  public ProjectFileHandler getProjectFile(final int projectId, final int version) {
    log.info(String.format("Fetching project file. project ID: %d version: %d", projectId, version));
    // TODO spyne: remove huge hack ! There should not be any special handling for Database Storage.
    if (storage instanceof DatabaseStorage) {
      return ((DatabaseStorage) storage).get(projectId, version);
    }

    /* Fetch meta data from db */
    final ProjectFileHandler pfh = projectLoader.fetchProjectMetaData(projectId, version);

    /* Fetch project file from storage and copy to local file */
    final String resourceId = requireNonNull(pfh.getResourceId(), String.format("URI is null. project ID: %d version: %d",
        pfh.getProjectId(), pfh.getVersion()));
    try (InputStream is = storage.get(resourceId)){
      final File file = createTempOutputFile(pfh);

      /* Copy from storage to output stream */
      try (FileOutputStream fos = new FileOutputStream(file)) {
        IOUtils.copy(is, fos);
      }

      /* Validate checksum */
      validateChecksum(file, pfh);

      /* Attach file to handler */
      pfh.setLocalFile(file);

      return pfh;
    } catch (IOException e) {
      throw new StorageException(e);
    }
  }

  private void validateChecksum(File file, ProjectFileHandler pfh) throws IOException {
    final byte[] hash = Md5Hasher.md5Hash(file);
    checkState(Arrays.equals(pfh.getMd5Hash(), hash),
        String.format("MD5 HASH Failed. project ID: %d version: %d Expected: %s Actual: %s",
            pfh.getProjectId(), pfh.getVersion(), new String(pfh.getMd5Hash()), new String(hash))
    );
  }

  private File createTempOutputFile(ProjectFileHandler projectFileHandler) throws IOException {
    return File.createTempFile(
        projectFileHandler.getFileName(),
        String.valueOf(projectFileHandler.getVersion()), tempDir);
  }
}
