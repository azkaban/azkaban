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

package azkaban.execapp;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import azkaban.executor.ExecutableFlow;
import azkaban.project.ProjectFileHandler;
import azkaban.project.ProjectManagerException;
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
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class FlowPreparer {

  // Name of the file which keeps project directory size
  static final String PROJECT_DIR_SIZE_FILE_NAME = "___azkaban_project_dir_size_in_bytes___";

  // Name of the file which keeps count of files inside the directory
  static final String PROJECT_DIR_COUNT_FILE_NAME = "___azkaban_project_dir_count___";

  private static final Logger log = Logger.getLogger(FlowPreparer.class);
  // TODO spyne: move to config class
  private final File executionsDir;
  // TODO spyne: move to config class
  private final File projectsDir;
  private final StorageManager storageManager;
  private final ProjectCacheDirCleaner projectDirCleaner;

  public FlowPreparer(final StorageManager storageManager, final File executionsDir,
      final File projectsDir, final Long projectDirMaxSizeInMb) {
    this.storageManager = storageManager;
    this.executionsDir = executionsDir;
    this.projectsDir = projectsDir;
    this.projectDirCleaner = new ProjectCacheDirCleaner(projectDirMaxSizeInMb);

  }

  /**
   * Creates a file which keeps the size of {@param dir} in bytes inside the {@param dir} and sets
   * the dirSize for {@param pv}.
   *
   * @param dir the directory whose size needs to be kept in the file to be created.
   * @param pv the projectVersion whose size needs to updated.
   */
  static void updateDirSize(final File dir, final ProjectVersion pv) {
    final long sizeInByte = FileUtils.sizeOfDirectory(dir);
    pv.setDirSizeInBytes(sizeInByte);
    try {
      FileIOUtils.dumpNumberToFile(Paths.get(dir.getPath(), PROJECT_DIR_SIZE_FILE_NAME),
          sizeInByte);
    } catch (final IOException e) {
      log.error("error when dumping dir size to file", e);
    }
  }

  /**
   * Creates a file which keeps the count of files inside {@param dir}
   *
   * @param dir the directory whose size needs to be kept in the file to be created.
   * @param pv the projectVersion whose size needs to updated.
   */
  static void updateFileCount(final File dir, final ProjectVersion pv) {
    final int fileCount = dir.listFiles().length;
    pv.setFileCount(fileCount);
    try {
      FileIOUtils.dumpNumberToFile(Paths.get(dir.getPath(), PROJECT_DIR_COUNT_FILE_NAME),
          fileCount);
    } catch (final IOException e) {
      log.error("error when dumping file count to file", e);
    }
  }

  /**
   * Prepare the flow directory for execution.
   *
   * @param flow Executable Flow instance.
   */
  synchronized void setup(final ExecutableFlow flow) {
    File execDir = null;
    try {
      // First get the ProjectVersion
      final ProjectVersion projectVersion = new ProjectVersion(flow.getProjectId(),
          flow.getVersion());

      // Setup the project
      setupProject(projectVersion);

      // Create the execution directory
      execDir = createExecDir(flow);
      // Create the symlinks from the project
      copyCreateHardlinkDirectory(projectVersion.getInstalledDir(), execDir);
      log.info(String
          .format("Flow Preparation complete. [execid: %d, path: %s]", flow.getExecutionId(),
              execDir.getPath()));
    } catch (final Exception e) {
      log.error("Error in setting up project directory: " + this.projectsDir + ", Exception: " + e);
      cleanup(execDir);
      throw new RuntimeException(e);
    }
  }

  private void cleanup(final File execDir) {
    if (execDir != null) {
      try {
        FileUtils.deleteDirectory(execDir);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Touch the file if it exists.
   *
   * @param path path to the target file
   */
  @VisibleForTesting
  void touchIfExists(final Path path) {
    try {
      Files.setLastModifiedTime(path, FileTime.fromMillis(System.currentTimeMillis()));
    } catch (final IOException ex) {
      log.error(ex);
    }
  }

  /**
   * Prepare the project directory.
   *
   * @param pv ProjectVersion object
   */
  @VisibleForTesting
  void setupProject(final ProjectVersion pv)
      throws ProjectManagerException, IOException {
    final int projectId = pv.getProjectId();
    final int version = pv.getVersion();

    final String projectDir = String.valueOf(projectId) + "." + String.valueOf(version);
    if (pv.getInstalledDir() == null) {
      pv.setInstalledDir(new File(this.projectsDir, projectDir));
    }

    // If directory exists. Assume its prepared and skip.
    if (pv.getInstalledDir().exists()) {
      log.info("Project already cached. Skipping download. " + pv);
      touchIfExists(
          Paths.get(pv.getInstalledDir().getPath(), PROJECT_DIR_SIZE_FILE_NAME));
      return;
    }

    log.info("Preparing Project: " + pv);

    final File tempDir = new File(this.projectsDir,
        "_temp." + projectDir + "." + System.currentTimeMillis());

    // TODO spyne: Why mkdirs? This path should be already set up.
    tempDir.mkdirs();

    ProjectFileHandler projectFileHandler = null;
    try {
      log.info(String.format("Downloading zip file for Project Version {%s}", pv));
      projectFileHandler = requireNonNull(this.storageManager.getProjectFile(projectId, version));
      checkState("zip".equals(projectFileHandler.getFileType()));
      final File zipFile = requireNonNull(projectFileHandler.getLocalFile());
      final ZipFile zip = new ZipFile(zipFile);
      Utils.unzip(zip, tempDir);
      updateDirSize(tempDir, pv);
      updateFileCount(tempDir, pv);
      log.info(String.format("Downloading zip file for Project Version {%s} completes", pv));

      this.projectDirCleaner.deleteProjectDirsIfNecessary(pv.getDirSizeInBytes());
      Files.move(tempDir.toPath(), pv.getInstalledDir().toPath(), StandardCopyOption.ATOMIC_MOVE);

      log.warn(String.format("Project preparation completes. [%s]", pv));
    } finally {
      if (projectFileHandler != null) {
        projectFileHandler.deleteLocalFile();
      }
      // Clean up: Remove tempDir if exists
      FileUtils.deleteDirectory(tempDir);
    }
  }

  private void copyCreateHardlinkDirectory(final File projectDir, final File execDir)
      throws IOException {
    FileIOUtils.createDeepHardlink(projectDir, execDir);
  }

  private File createExecDir(final ExecutableFlow flow) {
    final int execId = flow.getExecutionId();
    final File execDir = new File(this.executionsDir, String.valueOf(execId));
    flow.setExecutionPath(execDir.getPath());

    // TODO spyne: Why mkdirs? This path should be already set up.
    execDir.mkdirs();
    return execDir;
  }


  private class ProjectCacheDirCleaner {

    private final Long projectDirMaxSizeInMb;

    /*
     * Delete the project dir associated with {@code version}.
     * It first acquires object lock of {@code version} waiting for other threads creating
     * execution dir to finish to avoid race condition. An example of race condition scenario:
     * delete the dir of a project while an execution of a flow in the same project is being setup
     * and the flow's execution dir is being created({@link FlowPreparer#setup}).
     */
    private void deleteDirectory(final ProjectVersion pv) throws IOException {
      synchronized (pv.toString().intern()) {
        FlowPreparer.log.warn("Deleting project: " + pv);
        final File installedDir = pv.getInstalledDir();
        if (installedDir != null && installedDir.exists()) {
          FileUtils.deleteDirectory(installedDir);
        }
      }
    }

    ProjectCacheDirCleaner(final Long projectDirMaxSizeInMb) {
      this.projectDirMaxSizeInMb = projectDirMaxSizeInMb;
    }

    private List<Path> loadAllProjectDirs() {
      final List<Path> projects = new ArrayList<>();
      for (final File project : FlowPreparer.this.projectsDir.listFiles(new FilenameFilter() {

        String pattern = "[0-9]+\\.[0-9]+";

        @Override
        public boolean accept(final File dir, final String name) {
          return name.matches(this.pattern);
        }
      })) {
        if (project.exists() && project.isDirectory()) {
          projects.add(project.toPath());
        } else {
          FlowPreparer.log
              .debug(String.format("project %s doesn't exist or is non-dir.", project.getName()));
        }
      }
      return projects;
    }

    private List<ProjectVersion> loadAllProjects() {
      final List<ProjectVersion> allProjects = new ArrayList<>();
      for (final Path project : this.loadAllProjectDirs()) {
        try {
          final String fileName = project.getFileName().toString();
          final int projectId = Integer.parseInt(fileName.split("\\.")[0]);
          final int versionNum = Integer.parseInt(fileName.split("\\.")[1]);
          final ProjectVersion projVersion =
              new ProjectVersion(projectId, versionNum, project.toFile());
          final Path projectDirSizeFile = Paths
              .get(projVersion.getInstalledDir().toString(),
                  FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME);
          if (!Files.exists(projectDirSizeFile)) {
            FlowPreparer.updateDirSize(projVersion.getInstalledDir(), projVersion);
          }

          final Path projectDirFileCount = Paths.get(projVersion.getInstalledDir().toString(),
              FlowPreparer.PROJECT_DIR_COUNT_FILE_NAME);
          if (!Files.exists(projectDirFileCount)) {
            FlowPreparer.updateFileCount(projVersion.getInstalledDir(), projVersion);
          }

          projVersion.setLastAccessTime(Files.getLastModifiedTime(projectDirSizeFile));
          allProjects.add(projVersion);
        } catch (final Exception e) {
          FlowPreparer.log
              .error(String.format("error while loading project dir metadata for project %s",
                  project.getFileName()), e);
        }
      }
      return allProjects;
    }

    /**
     * @return sum of the size of all project dirs
     */
    private long getProjectDirsTotalSizeInBytes(final List<ProjectVersion> allProjects) {
      long totalSizeInBytes = 0;
      for (final ProjectVersion version : allProjects) {
        totalSizeInBytes += version.getDirSizeInBytes();
      }
      return totalSizeInBytes;
    }

    private void deleteLeastRecentlyUsedProjects(long sizeToFreeInBytes,
        final List<ProjectVersion> projectVersions) {
      // sort project version by last reference time in ascending order
      projectVersions.sort(Comparator.comparing(ProjectVersion::getLastAccessTime));
      for (final ProjectVersion version : projectVersions) {
        if (sizeToFreeInBytes > 0) {
          try {
            // delete the project directory even if flow within is running. It's OK to
            // delete the directory since execution dir is HARD linked to project dir.
            deleteDirectory(version);
            sizeToFreeInBytes -= version.getDirSizeInBytes();
          } catch (final IOException ex) {
            FlowPreparer.log.error(ex);
          }
        }
      }
    }

    void deleteProjectDirsIfNecessary(final long spaceToDeleteInBytes) {
      if (this.projectDirMaxSizeInMb != null) {
        final long start = System.currentTimeMillis();
        final List<ProjectVersion> allProjects = loadAllProjects();
        FlowPreparer.log
            .debug(String.format("loading all project dirs metadata completes in %s sec(s)",
                Duration.ofSeconds(System.currentTimeMillis() - start).getSeconds()));

        final long currentSpaceInBytes = getProjectDirsTotalSizeInBytes(allProjects);
        if (currentSpaceInBytes + spaceToDeleteInBytes
            >= this.projectDirMaxSizeInMb * 1024 * 1024) {
          FlowPreparer.log.info(String.format("Project dir disk usage[%s bytes] exceeds the "
                  + "limit, start cleaning up project dirs",
              currentSpaceInBytes + spaceToDeleteInBytes));
          deleteLeastRecentlyUsedProjects(spaceToDeleteInBytes, allProjects);
        }
      }
    }
  }
}
