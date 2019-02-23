/*
 * Copyright 2019 LinkedIn Corp.
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
 */

package azkaban.execapp;

import azkaban.utils.ExecutorServiceUtils;
import azkaban.utils.FileIOUtils;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for deleting least recently accessed projects in the shared project
 * cache when there's no room to accommodate a new project.
 */
class ProjectCacheCleaner {

  private final File projectCacheDir;
  private final long projectCacheMaxSizeInMB;
  private static final Logger log = LoggerFactory.getLogger(ProjectCacheCleaner.class);

  ProjectCacheCleaner(final File projectCacheDir, final long projectCacheMaxSizeInMB) {
    Preconditions.checkNotNull(projectCacheDir);
    Preconditions.checkArgument(projectCacheDir.exists());
    Preconditions.checkArgument(projectCacheMaxSizeInMB > 0);

    this.projectCacheDir = projectCacheDir;
    this.projectCacheMaxSizeInMB = projectCacheMaxSizeInMB;
  }

  /**
   * @return a list of project directories.
   */
  private List<Path> loadAllProjectDirs() {
    final List<Path> projects = new ArrayList<>();
    for (final File project : this.projectCacheDir.listFiles(new FilenameFilter() {

      String pattern = "[0-9]+\\.[0-9]+";

      @Override
      public boolean accept(final File dir, final String name) {
        return name.matches(this.pattern);
      }
    })) {
      if (project.exists() && project.isDirectory()) {
        projects.add(project.toPath());
      } else {
        log.debug("project {} doesn't exist or is non-dir.", project.getName());
      }
    }
    return projects;
  }

  /**
   * @return a list of {@link ProjectDirectoryMetadata} for all project directories
   */
  private List<ProjectDirectoryMetadata> loadAllProjects() {
    final List<ProjectDirectoryMetadata> allProjects = new ArrayList<>();
    for (final Path project : this.loadAllProjectDirs()) {
      try {
        final String fileName = project.getFileName().toString();
        final int projectId = Integer.parseInt(fileName.split("\\.")[0]);
        final int versionNum = Integer.parseInt(fileName.split("\\.")[1]);

        final ProjectDirectoryMetadata projectDirMetadata =
            new ProjectDirectoryMetadata(projectId, versionNum, project.toFile());

        projectDirMetadata.setDirSizeInByte(
            FlowPreparer.calculateDirSizeAndSave(projectDirMetadata.getInstalledDir()));
        projectDirMetadata.setLastAccessTime(
            Files.getLastModifiedTime(Paths.get(projectDirMetadata.getInstalledDir().toString(),
                FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME)));
        allProjects.add(projectDirMetadata);
      } catch (final Exception e) {
        log.warn("error while loading project dir metadata for project {}",
            project.getFileName(), e);
      }
    }
    return allProjects;
  }

  /**
   * @return sum of the size of all project dirs
   */
  private long getProjectDirsTotalSizeInBytes(final List<ProjectDirectoryMetadata> allProjects) {
    final long totalSizeInBytes = allProjects.stream()
        .mapToLong(ProjectDirectoryMetadata::getDirSizeInByte).sum();
    return totalSizeInBytes;
  }

  /**
   * Delete all the files in parallel
   *
   * @param filesToDelete a list of files to delete
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  private void deleteFilesInParallel(final List<File> filesToDelete) {
    final int CLEANING_SERVICE_THREAD_NUM = 8;
    final ExecutorService deletionService = Executors
        .newFixedThreadPool(CLEANING_SERVICE_THREAD_NUM);

    for (final File toDelete : filesToDelete) {
      deletionService.submit(() -> FileIOUtils.deleteDirectorySilently(toDelete));
    }

    try {
      new ExecutorServiceUtils().gracefulShutdown(deletionService, Duration.ofDays(1));
    } catch (final InterruptedException e) {
      log.warn("error when deleting files", e);
    }
  }

  /**
   * Delete least recently used projects to free up space
   *
   * @param sizeToFreeInBytes space to free up
   * @param projectDirMetadataList a list of candidate files to delete
   */
  private void deleteLeastRecentlyUsedProjects(long sizeToFreeInBytes,
      final List<ProjectDirectoryMetadata> projectDirMetadataList) {
    // Sort projects by last reference time in ascending order
    projectDirMetadataList.sort(Comparator.comparing(ProjectDirectoryMetadata::getLastAccessTime));
    final List<File> filesToDelete = new ArrayList<>();

    for (final ProjectDirectoryMetadata proj : projectDirMetadataList) {
      if (sizeToFreeInBytes > 0) {
        // Delete the project directory even if flow within is running. It's OK to
        // delete the directory since execution dir is HARD linked to project dir. Note that even
        // if project is deleted, disk space will be freed up only when all associated execution
        // dirs are deleted.
        log.debug("deleting project {}", proj);
        if (proj.getInstalledDir() != null) {
          filesToDelete.add(proj.getInstalledDir());
          sizeToFreeInBytes -= proj.getDirSizeInByte();
        }
      } else {
        break;
      }
    }
    deleteFilesInParallel(filesToDelete);
  }

  /**
   * Deleting least recently accessed project dirs when there's no room to accommodate new project
   */
  void deleteProjectDirsIfNecessary(final long newProjectSizeInBytes) {
    final long start = System.currentTimeMillis();
    final List<ProjectDirectoryMetadata> allProjects = loadAllProjects();
    log.debug("loading all project dirs metadata completed in {} sec(s)",
        Duration.ofSeconds(System.currentTimeMillis() - start).getSeconds());

    final long currentSpaceInBytes = getProjectDirsTotalSizeInBytes(allProjects);
    if (currentSpaceInBytes + newProjectSizeInBytes
        >= this.projectCacheMaxSizeInMB * 1024 * 1024) {
      log.info(
          "project cache usage[{} MB] exceeds the limit[{} MB], start cleaning up project dirs",
          (currentSpaceInBytes + newProjectSizeInBytes) / (1024 * 1024),
          this.projectCacheMaxSizeInMB);

      final long freeCacheSpaceInBytes =
          this.projectCacheMaxSizeInMB * 1024 * 1024 - currentSpaceInBytes;

      deleteLeastRecentlyUsedProjects(newProjectSizeInBytes - freeCacheSpaceInBytes, allProjects);
    }
  }
}
