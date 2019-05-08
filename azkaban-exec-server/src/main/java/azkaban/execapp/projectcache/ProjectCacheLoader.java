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

package azkaban.execapp.projectcache;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import azkaban.execapp.lockingcache.LockingCacheLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.project.ProjectFileHandler;
import azkaban.storage.StorageManager;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Utils;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** project cache loader */
public class ProjectCacheLoader implements LockingCacheLoader<ProjectCacheKey,
    ProjectDirectoryInfo> {
  private final File projectCacheDir;
  private final StorageManager storageManager;
  // Name of the file which keeps project directory size

  static final String PROJECT_DIR_SIZE_FILE_NAME = "___azkaban_project_dir_size_in_bytes___";
  private static final Logger log = LoggerFactory.getLogger(ProjectCacheLoader.class);

  public ProjectCacheLoader(StorageManager storageManager, File projectCacheDir) {
    this.storageManager = storageManager;
    this.projectCacheDir = projectCacheDir;
  }

  @Override
  public ProjectDirectoryInfo load(ProjectCacheKey key) throws Exception {
    final String projectDir = generateProjectDirName(key);
    File installedDir = new File(projectCacheDir, projectDir);
    if (installedDir.exists()) {
      if (installedDir.isDirectory()) {
        log.warn("{} aleady exists.", installedDir.getAbsolutePath());
        long size = calculateDirSizeAndSave(installedDir);
        return new ProjectDirectoryInfo(key, installedDir, size);
      }
      else {
        log.error("{} is not a valid project directory", installedDir.getAbsolutePath());
        throw new ExecutorManagerException("invalid project directory for " + key);
      }
    }
    long size = downloadProject(key, installedDir);

    return new ProjectDirectoryInfo(key, installedDir, size);
  }

  @Override
  public Map<ProjectCacheKey, ProjectDirectoryInfo> loadAll() throws ExecutorManagerException {
    final Map<ProjectCacheKey, ProjectDirectoryInfo> projects = new HashMap<>();
    final String projectDirPattern = "[0-9]+\\.[0-9]+";
    final File files[] = this.projectCacheDir.listFiles(
        (dir, name) -> (name != null) && name.matches(projectDirPattern));

    for (final File project : files) {
      if (project.exists() && project.isDirectory()) {
        try {
          String fileName = project.getName();
          final int projectId = Integer.parseInt(fileName.split("\\.")[0]);
          final int versionNum = Integer.parseInt(fileName.split("\\.")[1]);
          final ProjectCacheKey key = new ProjectCacheKey(projectId, versionNum);
          final long size = calculateDirSizeAndSave(project);
          final ProjectDirectoryInfo projectDirInfo =
              new ProjectDirectoryInfo(key, project, size);
          projects.put(key, projectDirInfo);
        } catch (final Exception e) {
          log.error("error while loading project dir metadata for project {}",
              project.getName(), e);
          throw new ExecutorManagerException("Error loading existing project from " +
              project.getName() );
        }
      } else {
        log.error("project {} is not a proper project directory or has been removed.", project
            .getName());
      }
    }
    return projects;
  }

  @Override
  public void remove(ProjectCacheKey key, ProjectDirectoryInfo value) throws Exception {
    log.info("deleting project {} from cache.", key);
    FileIOUtils.deleteDirectorySilently(value.getDirectory());
  }

  /**
   * @return the project directory name of a project
   */
  private String generateProjectDirName(final ProjectCacheKey key) {
    return String.valueOf(key.getProjectId()) + "." + String.valueOf(key.getVersion());
  }

  /** create a temporary directory for the project */
  private File createTempDir(final ProjectCacheKey key) {
    final String projectDir = generateProjectDirName(key);
    final File tempDir = new File(this.projectCacheDir,
        "_temp." + projectDir + "." + UUID.randomUUID());
    tempDir.mkdirs();
    return tempDir;
  }

  /**
   * Download and unzip the project.
   * @param key project ID and version
   * @param dest the directory to download the project to
   * @return the size of the project directory
   * @throws IOException
   */
  private long downloadAndUnzipProject(final ProjectCacheKey key, final File dest)
      throws IOException {
    final ProjectFileHandler projectFileHandler = requireNonNull(this.storageManager
        .getProjectFile(key.getProjectId(), key.getVersion()));
    try {
      checkState("zip".equals(projectFileHandler.getFileType()));
      final File zipFile = requireNonNull(projectFileHandler.getLocalFile());
      final ZipFile zip = new ZipFile(zipFile);
      Utils.unzip(zip, dest);
      return calculateDirSizeAndSave(dest);
    } finally {
      projectFileHandler.deleteLocalFile();
    }
  }

  /**
   * Calculate the directory size and save it to a file.
   *
   * @param dir the directory whose size needs to be saved.
   * @return the size of the dir.
   */
  private long calculateDirSizeAndSave(final File dir) throws IOException {
    final Path path = Paths.get(dir.getPath(), PROJECT_DIR_SIZE_FILE_NAME);
    if (!Files.exists(path)) {
      final long sizeInByte = FileUtils.sizeOfDirectory(dir);
      FileIOUtils.dumpNumberToFile(path, sizeInByte);
      return sizeInByte;
    } else {
      return FileIOUtils.readNumberFromFile(path);
    }
  }

  /**
   * Download project zip and unzip it.
   *
   * @param key project key with project id and version
   * @return the temp dir where the new project is downloaded to, null if no project is downloaded.
   * @throws IOException if downloading or unzipping fails.
   */
  @VisibleForTesting
  long downloadProject(final ProjectCacheKey key, File installedDir) throws
      ExecutorManagerException {
    if (installedDir.exists()) {
      // If directory exists, then another process or thread must have downloaded it. This is
      // unexpected, since we should have a write lock to prevent anyone else from updating the
      // project directory. Either there is another executor using the same shared cache directory,
      // or there is some other bug allowing a thread to sneak in.
      log.error("ERROR: Project {} version {} already exists. Another executor may be "
          + "downloading to this cache. Proper locking cannot be guaranteed with multiple "
          + "executors sharing the cache.", key.getProjectId(), key.getProjectId());
      throw new ExecutorManagerException("Project " + key.getProjectId() + " version " + key
          .getVersion() + " already exists in the shared cache directory.");
    }
    final long start = System.currentTimeMillis();
    // Download project to a temp dir if it does not exist in local cache.
    final File tempDir = createTempDir(key);
    try {
      long size = downloadAndUnzipProject(key, tempDir);
      // Rename temp dir to a proper project directory name.
      Files.move(tempDir.toPath(), installedDir.toPath());

      log.info("Downloading zip file for project {}, size {}, completed in {} second(s)", key,
          size, (System.currentTimeMillis() - start) / 1000.0);
      return size;
    } catch (final Exception ex) {
      FileIOUtils.deleteDirectorySilently(tempDir);
      log.error("Error in downloading project {}", key);
      throw new ExecutorManagerException(ex);
    }
  }
}

