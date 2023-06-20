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
import azkaban.user.User;
import java.io.File;
import java.util.List;


/**
 * StorageManager manages and coordinates all project related interactions with the Storage layer. This also
 * includes bookkeeping like updating DB with the new versionm, etc
 */
public interface ProjectStorageManager {

  /**
   * API to a project file into Azkaban Storage
   *
   * TODO clean up interface
   *
   * @param project project
   * @param version The new version to be uploaded
   * @param localFile local file
   * @param uploader the user who uploaded
   */
  void uploadProject(
      final Project project,
      final int version,
      final File localFile,
      final File startupDependencies,
      final User uploader,
      final String uploaderIPAddr);

  /**
   * Clean up project artifacts of a given project id, except those with the project versions
   * provided.
   */
  void cleanupProjectArtifacts(final int projectId, final List<Integer> versionsToExclude);

  /**
   * Fetch project file from storage.
   *
   * @param projectId required project ID
   * @param version version to be fetched
   * @return Handler object containing hooks to fetched project file
   */
  ProjectFileHandler getProjectFile(final int projectId, final int version);
}
