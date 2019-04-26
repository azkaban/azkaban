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

import static azkaban.Constants.ConfigurationKeys.AZKABAN_STORAGE_ARTIFACT_MAX_RETENTION;
import static com.google.common.base.Preconditions.checkArgument;

import azkaban.db.DatabaseOperator;
import azkaban.spi.Storage;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class StorageCleaner {

  // Delete records of all older versions
  static final String SQL_DELETE_RESOURCE_ID = "DELETE FROM project_versions WHERE resource_id=?";

  /**
   * The query must sort the versions in reverse order for the cleanup operation to work correctly!
   * TODO spyne: Refactor database storage cleanup to use this
   *
   * When using DatabaseStorage, resourceId is always NULL. Hence, those rows will currently be
   * never cleaned up.
   */
  static final String SQL_FETCH_PVR = "SELECT resource_id FROM project_versions WHERE project_id=? AND resource_id IS NOT NULL ORDER BY version DESC";

  private static final Logger LOG = LoggerFactory.getLogger(StorageCleaner.class);
  private final DatabaseOperator databaseOperator;
  private final int maxArtifactsPerProject;
  private final Storage storage;

  @Inject
  public StorageCleaner(final Props props, final Storage storage,
      final DatabaseOperator databaseOperator) {
    this.storage = storage;
    this.databaseOperator = databaseOperator;

    this.maxArtifactsPerProject = props.getInt(AZKABAN_STORAGE_ARTIFACT_MAX_RETENTION, 0);
    checkArgument(this.maxArtifactsPerProject >= 0,
        String.format("Invalid value for %s : %d", AZKABAN_STORAGE_ARTIFACT_MAX_RETENTION,
            this.maxArtifactsPerProject));

    if (isCleanupPermitted()) {
      LOG.info(String.format("%s Config: Max %d artifact(s) retained per project",
          AZKABAN_STORAGE_ARTIFACT_MAX_RETENTION, this.maxArtifactsPerProject));
    } else {
      LOG.warn("Project cleanup disabled. All artifacts will be stored.");
    }
  }

  @VisibleForTesting
  boolean isCleanupPermitted() {
    return this.maxArtifactsPerProject > 0;
  }

  /**
   * Remove all but last N artifacts as configured by AZKABAN_STORAGE_ARTIFACT_MAX_RETENTION
   *
   * Since multiple versions can share the same filename, the algo is to collect all filenames and
   * from them, remove the latest ones. The remaining ones are deleted by the respective storage.
   *
   * From the storage perspective, cleanup just needs the {@link Storage#delete(String)} API to
   * work.
   *
   * Failure cases: - If the storage cleanup fails, the cleanup will be attempted again on the next
   * upload - If the storage cleanup succeeds and the DB cleanup fails, the DB will be cleaned up in
   * the next attempt.
   *
   * @param projectId project ID
   */
  public void cleanupProjectArtifacts(final int projectId) {
    if (!isCleanupPermitted()) {
      return;
    }
    final Set<String> allResourceIds = findResourceIdsToDelete(projectId);
    if (allResourceIds.size() == 0) {
      return;
    }

    LOG.warn(String.format("Deleting project artifacts [id: %d]: %s", projectId, allResourceIds));
    allResourceIds.forEach(this::delete);
  }

  private Set<String> findResourceIdsToDelete(final int projectId) {
    final List<String> resourceIdOrderedList = fetchResourceIdOrderedList(projectId);
    if (resourceIdOrderedList.size() <= this.maxArtifactsPerProject) {
      return Collections.emptySet();
    }

    final Set<String> allResourceIds = new HashSet<>(resourceIdOrderedList);
    final Set<String> doNotDeleteSet = new HashSet<>(
        resourceIdOrderedList.subList(0, this.maxArtifactsPerProject));
    allResourceIds.removeAll(doNotDeleteSet);
    return allResourceIds;
  }

  /**
   * Main Delete Utility.
   *
   * Delete the storage first. Then remove metadata from DB. Warning! This order cannot be reversed
   * since if the metadata is lost, there is no reference of the storage blob.
   *
   * @param resourceId the storage key to be deleted.
   * @return true if deletion was successful. false otherwise
   */
  private boolean delete(final String resourceId) {
    final boolean isDeleted = this.storage.delete(resourceId) && removeDbEntry(resourceId);
    if (!isDeleted) {
      LOG.info("Failed to delete resourceId: " + resourceId);
    }
    return isDeleted;
  }

  private boolean removeDbEntry(final String resourceId) {
    try {
      final int nAffectedRows = this.databaseOperator.update(SQL_DELETE_RESOURCE_ID, resourceId);
      return nAffectedRows > 0;
    } catch (final SQLException e) {
      LOG.error("Error while deleting DB metadata resource ID: " + resourceId, e);
    }
    return false;
  }

  private List<String> fetchResourceIdOrderedList(final int projectId) {
    try {
      return this.databaseOperator.query(SQL_FETCH_PVR,
          rs -> {
            final List<String> results = new ArrayList<>();
            while (rs.next()) {
              results.add(rs.getString(1));
            }
            return results;
          }, projectId);
    } catch (final SQLException e) {
      LOG.error("Error performing cleanup of Project: " + projectId, e);
    }
    return Collections.emptyList();
  }
}
