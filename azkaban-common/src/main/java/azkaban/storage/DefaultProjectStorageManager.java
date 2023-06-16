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
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import azkaban.project.Project;
import azkaban.project.ProjectFileHandler;
import azkaban.project.ProjectLoader;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.spi.ProjectStorageMetadata;
import azkaban.user.User;
import azkaban.utils.HashUtils;
import azkaban.utils.Props;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

@Singleton
public class DefaultProjectStorageManager implements ProjectStorageManager {

  private static final Logger log = Logger.getLogger(DefaultProjectStorageManager.class);

  private final StorageCleaner storageCleaner;
  private final Storage storage;
  private final ProjectLoader projectLoader;
  private final File tempDir;

  @Inject
  public DefaultProjectStorageManager(final Props props, final Storage storage,
      final ProjectLoader projectLoader,
      final StorageCleaner storageCleaner) {
    this.tempDir = new File(props.getString("project.temp.dir", "temp"));
    this.storage = requireNonNull(storage, "storage is null");
    this.projectLoader = requireNonNull(projectLoader, "projectLoader is null");
    this.storageCleaner = requireNonNull(storageCleaner, "storageCleanUp is null");

    prepareTempDir();
  }

  private void prepareTempDir() {
    if (!this.tempDir.exists()) {
      this.tempDir.mkdirs();
    }
    checkArgument(this.tempDir.isDirectory());
  }

  @Override
  public void uploadProject(
      final Project project,
      final int version,
      final File localFile,
      final File startupDependencies,
      final User uploader,
      final String uploaderIPAddr) {
    byte[] md5 = null;
    if (!(this.storage instanceof DatabaseStorage)) {
      md5 = computeHash(localFile);
    }
    final ProjectStorageMetadata metadata = new ProjectStorageMetadata(
        project.getId(), version, uploader.getUserId(), md5, uploaderIPAddr);
    log.info(String.format("Adding archive to storage. Meta:%s File: %s[%d bytes]",
        metadata, localFile.getName(), localFile.length()));

    /* upload to storage */
    final String resourceId = this.storage.putProject(metadata, localFile);

    /* Add metadata to db */
    // TODO spyne: remove hack. Database storage should go through the same flow
    if (!(this.storage instanceof DatabaseStorage)) {
      this.projectLoader.addProjectVersion(
          project.getId(),
          version,
          localFile,
          startupDependencies,
          uploader.getUserId(),
          requireNonNull(md5),
          requireNonNull(resourceId),
          uploaderIPAddr
      );
      log.info(String.format("Added project metadata to DB. Meta:%s File: %s[%d bytes] URI: %s",
          metadata, localFile.getName(), localFile.length(), resourceId));
    }
  }

  @Override
  public void cleanupProjectArtifacts(final int projectId, final List<Integer> versionsToExclude) {
    try {
      this.storageCleaner.cleanupProjectArtifacts(projectId, versionsToExclude);
    } catch (final Exception e) {
      log.error("Error occured during cleanup. Ignoring and continuing...", e);
    }
  }

  private byte[] computeHash(final File localFile) {
    final byte[] md5;
    try {
      md5 = HashUtils.MD5.getHashBytes(localFile);
    } catch (final IOException e) {
      throw new StorageException(e);
    }
    return md5;
  }

  @Override
  public ProjectFileHandler getProjectFile(final int projectId, final int version) {
    log.info(
        String.format("Fetching project file. project ID: %d version: %d", projectId, version));
    // TODO spyne: remove huge hack ! There should not be any special handling for Database Storage.
    if (this.storage instanceof DatabaseStorage) {
      return ((DatabaseStorage) this.storage).getProject(projectId, version);
    }

    /* Fetch meta data from db */
    final ProjectFileHandler pfh = this.projectLoader.fetchProjectMetaData(projectId, version);

    /* Fetch project file from storage and copy to local file */
    final String resourceId = requireNonNull(pfh.getResourceId(),
        String.format("URI is null. project ID: %d version: %d",
            pfh.getProjectId(), pfh.getVersion()));

    try (final InputStream is = this.storage.getProject(resourceId)) {
      final File file = createTempOutputFile(pfh);

      /* Copy from storage to output stream */
      try (final FileOutputStream fos = new FileOutputStream(file)) {
        IOUtils.copy(is, fos);
      }

      /* Validate checksum */
      validateChecksum(file, pfh);

      /* Attach file to handler */
      pfh.setLocalFile(file);

      return pfh;
    } catch (final IOException e) {
      throw new StorageException(e);
    }
  }

  private void validateChecksum(final File file, final ProjectFileHandler pfh) throws IOException {
    final byte[] hash = HashUtils.MD5.getHashBytes(file);
    checkState(HashUtils.isSameHash(pfh.getMD5Hash(), hash),
        String.format("MD5 HASH Failed. project ID: %d version: %d Expected: %s Actual: %s",
            pfh.getProjectId(), pfh.getVersion(), HashUtils.bytesHashToString(pfh.getMD5Hash()),
            HashUtils.bytesHashToString(hash))
    );
  }

  private File createTempOutputFile(final ProjectFileHandler projectFileHandler)
      throws IOException {
    return File.createTempFile(
        projectFileHandler.getFileName(),
        String.valueOf(projectFileHandler.getVersion()), this.tempDir);
  }
}
