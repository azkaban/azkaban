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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for deleting least recently accessed projects in the shared project
 * cache when there's no room to accommodate a new project.
 */
class ProjectCacheCleaner {

  // Root directory for project cache
  private final File projectCacheDir;

  // cache size in percentage of disk partition where {@link projectCacheDir} belongs to
  private final double percentageOfDisk;

  private static final Logger log = LoggerFactory.getLogger(ProjectCacheCleaner.class);

  // Number of threads in the cache cleanup service
  private static final int CLEANING_SERVICE_THREAD_NUM = 8;

  private static final double DEFAULT_THROTTLE_PERCENTAGE = 0.92; // 92%

  // If space in Cache partition goes above this Percentage, incoming request must wait
  // till the current cache cleanup cycle is done
  private double throttlePercentage;

  // Currently cached projects
  private final Map<Path, ProjectDirectoryMetadata> cachedProjects = new HashMap<>();

  // A record of projects under deletion
  private final ConcurrentMap<Path, File> projectsUnderDeletion = new ConcurrentHashMap<>();

  // Executor service responsible for cache cleanup
  private ExecutorService deletionService;

  // This is leveraged as a barrier mechanism to stall an incoming
  // request until ongoing cache cleanup cycle is done. This is only necessary if new projects
  // get added very rapidly driving cache space above throttlePercentage
  private final Lock barrier = new ReentrantLock();
  private Condition emptyQCond;

  public static final String STATE_AVAILABLE = "CACHE_AVAILABLE";
  public static final String STATE_CLEANING = "CACHE_CLEANING";

  public ProjectCacheCleaner(final File projectCacheDir, final double percentageOfDisk) {
    this(projectCacheDir, percentageOfDisk, DEFAULT_THROTTLE_PERCENTAGE);
  }

  public ProjectCacheCleaner(final File projectCacheDir, final double percentageOfDisk,
      final double throttlePercentage) {
    Preconditions.checkNotNull(projectCacheDir);
    Preconditions.checkArgument(projectCacheDir.exists());
    Preconditions.checkArgument(percentageOfDisk > 0 && percentageOfDisk <= 1);
    this.projectCacheDir = projectCacheDir;
    this.percentageOfDisk = percentageOfDisk;
    this.throttlePercentage = throttlePercentage;

    log.info("ProjectCacheCleaner constructor called. ProjectCacheDir = {}, thresh-hold = {} %, throttle at {} %",
        projectCacheDir.toPath(), this.percentageOfDisk, this.throttlePercentage);

    emptyQCond = barrier.newCondition();
    deletionService = Executors.newFixedThreadPool(CLEANING_SERVICE_THREAD_NUM);
  }

  /**
   * Get metadata from the OS for the underlying path, lastAccessTime is fetched from the OS
   * regardless of whether the given project already exists, but the space calculation for a
   * project directory is only performed one-time.
   *
   * @param project path for the project cache. Project filepath encodes projectID & version
   *                within the filename
   *
   * @return OS Metadata for the given path
   */
  private ProjectDirectoryMetadata fetchProjectMetadata(final Path project) {
    ProjectDirectoryMetadata projectDirectoryMetadata = this.cachedProjects.get(project);

    try {
      if (projectDirectoryMetadata == null) {
        final String fileName = project.getFileName().toString();
        final int projectId = Integer.parseInt(fileName.split("\\.")[0]);
        final int versionNum = Integer.parseInt(fileName.split("\\.")[1]);
        projectDirectoryMetadata = new ProjectDirectoryMetadata(projectId, versionNum, project.toFile());

        /*
         * Calculate used-space (Equivalent of du command) only if the metadata for
         * this project was never fetched before. This optimization is important as
         * recursive space calculation is a very expensive operation.
         */
        projectDirectoryMetadata.setDirSizeInByte(
            FlowPreparer.calculateDirSizeAndSave(projectDirectoryMetadata.getInstalledDir()));
      }

      projectDirectoryMetadata.setLastAccessTime(
          Files.getLastModifiedTime(Paths.get(projectDirectoryMetadata.getInstalledDir().toString(),
              FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME)));

    } catch (final Exception e) {
      log.warn("Error while loading project dir metadata for project {}",
          project.getFileName(), e);
    }
    return projectDirectoryMetadata;
  }

  /**
   * Browse Cache root directory to fetch all valid projects and unclean files. If a project
   * already exists in the cache, don't bother to re-fetch the OS metadata again.
   */
  private void loadAllProjects() {
    final List<Path> projects = new ArrayList<>();
    for (final File project : this.projectCacheDir.listFiles(new FilenameFilter() {

      String pattern = "[0-9]+\\.[0-9]+";

      @Override
      public boolean accept(final File dir, final String name) {
        return name.matches(this.pattern);
      }
    })) {
      if (project.exists() && project.isDirectory()) {
        if (!projectsUnderDeletion.containsKey(project.toPath())) {
          ProjectDirectoryMetadata projectDirectoryMetadata = fetchProjectMetadata(project.toPath());
          if (projectDirectoryMetadata != null) {
            cachedProjects.put(project.toPath(), projectDirectoryMetadata);
          }
        }
      }
    }
  }

  /**
   * @return sum of the size of all project dirs
   */
  private long getProjectDirsTotalSizeInBytes() {
    long totalSizeInBytes = 0;
    for (ProjectDirectoryMetadata metadata : cachedProjects.values()) {
      totalSizeInBytes += metadata.getDirSizeInByte();
    }
    return totalSizeInBytes;
  }

  private void addToDeletionQueue(final File toDelete) {
    try {
      barrier.lock();
      projectsUnderDeletion.put(toDelete.toPath(), toDelete);
    } finally {
      barrier.unlock();
    }
  }

  private void removeFromDeletionQueue(final Path toDelete) {
    try {
      barrier.lock();
      projectsUnderDeletion.remove(toDelete);
      emptyQCond.signal();
    } finally {
      barrier.unlock();
    }
  }

  /**
   * Submit a project directory for deletion
   *
   * @param toDelete project dir for deletion
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  private void submitProjectForDeletion(final File toDelete) {
      addToDeletionQueue(toDelete);
      deletionService.submit(() -> {
        log.info("Deleting project dir {} from project cache to free up space", toDelete);

        final long start = System.currentTimeMillis();
        FileIOUtils.deleteDirectorySilently(toDelete);
        log.info("Deleting project dir {} completed in {} msec(s)", toDelete, System.currentTimeMillis() - start);
        removeFromDeletionQueue(toDelete.toPath());
      });
  }

  /**
   *
   * Delete least recently used projects to free up space
   *
   * @param sizeToFreeInBytes space to free up
   */
  private void deleteLeastRecentlyUsedProjects(long sizeToFreeInBytes) {

    final List<ProjectDirectoryMetadata> lruList = new ArrayList<>(cachedProjects.values());
    lruList.sort(Comparator.comparing(ProjectDirectoryMetadata::getLastAccessTime));
    for (ProjectDirectoryMetadata lruEntry : lruList) {
      if (sizeToFreeInBytes > 0) {
        if (lruEntry.getInstalledDir() != null) {
          cachedProjects.remove(lruEntry.getInstalledDir().toPath());
          submitProjectForDeletion(lruEntry.getInstalledDir());
          sizeToFreeInBytes -= lruEntry.getDirSizeInByte();
        }
      } else {
        break;
      }
    }
  }

  private long bytesToMB(final long bytes) {
    return bytes / (1024 * 1024);
  }

  /**
   *
   * This method will block until all active cleanup threads finish deleting submitted
   * cleanup jobs.
   */
  @VisibleForTesting
  void finishPendingCleanup() {
    final long start = System.currentTimeMillis();
    try {
      try {
        this.barrier.lock();
        while (!projectsUnderDeletion.isEmpty()) {
          log.info("{} entries left in the cache directory deletion Q. Waiting for the cleanup to finish",
              this.projectsUnderDeletion.size());
          this.emptyQCond.await(10, TimeUnit.SECONDS);
        }
      } finally {
        this.barrier.unlock();
      }
      log.info("Took {} ms to complete ongoing cache cleanup.", (System.currentTimeMillis() - start));
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Deleting least recently accessed project dirs when there's no room to accommodate new project.
   *
   * The logic:
   * 1. Calculates the total dynamic size available for the project cache.
   *    This = (Usable space left in the disk partition + Space currently occupied by the project cache).
   * 2. Calculates high water mark & throttle water marks based on the above number.
   * 3. If the occupied bytes > high water mark, lazy (Non-blocking) LRU eviction kicks in
   * 4. If the occupied bytes > throttle water mark, the method will block until LRU eviction is complete.
   * In each case, LRU eviction attempts to keep the occupied space below high water mark.
   *
   * @param newProjectSizeInBytes space in bytes the new project will add to the existing cache
   */
  public void deleteProjectDirsIfNecessary(final long newProjectSizeInBytes) {
    final long cachePartitionSize = this.projectCacheDir.getTotalSpace();
    final long availablePartitionSize = this.projectCacheDir.getUsableSpace();

    final long start = System.currentTimeMillis();
    loadAllProjects();
    log.info("Loading {} project dirs metadata completed in {} msecs",
        cachedProjects.size(), System.currentTimeMillis() - start);

    final long currentCacheSize = getProjectDirsTotalSizeInBytes();
    final long projectCacheDirCapacity = currentCacheSize + availablePartitionSize;
    boolean throttleAfterDeletion = false;

    final long highWatermark = (long) (projectCacheDirCapacity * this.percentageOfDisk);
    final long throttleWatermark = (long) (projectCacheDirCapacity * this.throttlePercentage);

    long projectedCacheSize = currentCacheSize + newProjectSizeInBytes;

    log.info("Partition = {} MB, Total Capacity = {} MB, Cache Size = {} MB, Projected Size = {} MB",
        bytesToMB(cachePartitionSize),
        bytesToMB(projectCacheDirCapacity),
        bytesToMB(currentCacheSize),
        bytesToMB(projectedCacheSize));
    log.info("High Watermark = {} MB, Throttle Watermark = {} MB",
        bytesToMB(highWatermark),
        bytesToMB(throttleWatermark));

    if (projectedCacheSize >= throttleWatermark) {
      throttleAfterDeletion = true;
    }

    if (projectedCacheSize >= highWatermark) {
      log.info("Projected cache size exceeds High Watermark. LRU Eviction will kick in");
      deleteLeastRecentlyUsedProjects(projectedCacheSize - highWatermark);
    }

    if (throttleAfterDeletion) {
      /*
       * Block till already submitted cleanup is done.
       */
      log.info("Throttle Watermark was hit. Blocking till LRU eviction is complete.");
      finishPendingCleanup();
    }
  }

  /**
   *
   * @return Return the current state of the cleaner service
   */
  public String queryState() {
    if (projectsUnderDeletion.isEmpty()) {
      return STATE_AVAILABLE;
    }
    return STATE_CLEANING;
  }

  /**
   * Makes sure the Cache deletion process cleanly terminates so the possibility of unclean
   * cache directories is eliminated.
   */
  public void shutdown() {
    try {
      new ExecutorServiceUtils().gracefulShutdown(deletionService, Duration.ofDays(1));
    } catch (final InterruptedException e) {
      log.warn("Error when deleting files", e);
    }
  }
}
