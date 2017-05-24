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

import azkaban.project.ProjectFileHandler;
import azkaban.project.ProjectLoader;
import azkaban.spi.Storage;
import azkaban.spi.StorageMetadata;
import java.io.File;
import java.io.InputStream;
import javax.inject.Inject;


/**
 * DatabaseStorage
 *
 * This class helps in storing projects in the DB itself. This is intended to be the default since it is the current
 * behavior of Azkaban.
 */
public class DatabaseStorage implements Storage {
  private final ProjectLoader projectLoader;

  @Inject
  public DatabaseStorage(ProjectLoader projectLoader) {
    this.projectLoader = projectLoader;
  }

  @Override
  public InputStream get(String key) {
    throw new UnsupportedOperationException("Not implemented yet. Use get(projectId, version) instead");
  }

  public ProjectFileHandler get(int projectId, int version) {
    return projectLoader.getUploadedFile(projectId, version);
  }

  @Override
  public String put(StorageMetadata metadata, File localFile) {
    projectLoader.uploadProjectFile(
        metadata.getProjectId(),
        metadata.getVersion(),
        localFile, metadata.getUploader());

    return null;
  }

  @Override
  public boolean delete(String key) {
    throw new UnsupportedOperationException("Delete is not supported");
  }
}
