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
import azkaban.utils.Pair;
import azkaban.utils.Utils;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class FlowPreparer {

  // Name of the file which keeps project directory size
  static final String PROJECT_DIR_SIZE_FILE_NAME = "___azkaban_project_dir_size_in_bytes___";
  private static final Logger log = Logger.getLogger(FlowPreparer.class);
  // TODO spyne: move to config class
  private final File executionsDir;
  // TODO spyne: move to config class
  private final File projectsDir;
  private final Map<Pair<Integer, Integer>, ProjectVersion> installedProjects;
  private final StorageManager storageManager;
  private final ProjectCacheDirCleaner projectDirCleaner;

  public FlowPreparer(final StorageManager storageManager, final File executionsDir,
      final File projectsDir,
      final Map<Pair<Integer, Integer>, ProjectVersion> installedProjects,
      final Long projectDirMaxSizeInMb) {
    this.storageManager = storageManager;
    this.executionsDir = executionsDir;
    this.projectsDir = projectsDir;
    this.installedProjects = installedProjects;
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
   * Prepare the flow directory for execution.
   *
   * @param flow Executable Flow instance.
   */
  void setup(final ExecutableFlow flow) {
    File execDir = null;
    try {
      // First get the ProjectVersion
      final ProjectVersion projectVersion = getProjectVersion(flow);

      // Setup the project
      setupProject(projectVersion);

      // Create the execution directory
      execDir = createExecDir(flow);

      // Synchronized on {@code projectVersion} to prevent one thread deleting a project dir
      // in {@link FlowPreparer#setup} while another is creating hardlink from the same project dir
      synchronized (projectVersion) {
        // Create the symlinks from the project
        copyCreateHardlinkDirectory(projectVersion.getInstalledDir(), execDir);
        log.info(String.format("Flow Preparation complete. [execid: %d, path: %s]",
            flow.getExecutionId(), execDir.getPath()));
      }
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
      projectFileHandler = requireNonNull(this.storageManager.getProjectFile(projectId, version));
      checkState("zip".equals(projectFileHandler.getFileType()));

      log.info("Downloading zip file.");
      final File zipFile = requireNonNull(projectFileHandler.getLocalFile());
      final ZipFile zip = new ZipFile(zipFile);
      Utils.unzip(zip, tempDir);
      updateDirSize(tempDir, pv);
      this.projectDirCleaner.deleteProjectDirsIfNecessary(pv.getDirSizeInBytes());
      Files.move(tempDir.toPath(), pv.getInstalledDir().toPath(), StandardCopyOption.ATOMIC_MOVE);
      this.installedProjects.put(new Pair<>(pv.getProjectId(), pv.getVersion()), pv);
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

  private ProjectVersion getProjectVersion(final ExecutableFlow flow) {
    // We're setting up the installed projects. First time, it may take a while
    // to set up.
    final ProjectVersion projectVersion;
    synchronized (this.installedProjects) {
      final Pair<Integer, Integer> pair = new Pair<>(flow.getProjectId(), flow.getVersion());
      projectVersion = this.installedProjects.getOrDefault(pair, new ProjectVersion(flow
          .getProjectId(), flow.getVersion()));
    }
    return projectVersion;
  }

  private class ProjectCacheDirCleaner {

    private final Long projectDirMaxSizeInMb;

    ProjectCacheDirCleaner(final Long projectDirMaxSizeInMb) {
      this.projectDirMaxSizeInMb = projectDirMaxSizeInMb;
    }

    /**
     * @return sum of the size of all project dirs
     */
    private long getProjectDirsTotalSizeInBytes() throws IOException {
      long totalSizeInBytes = 0;
      for (final ProjectVersion version : FlowPreparer.this.installedProjects.values()) {
        totalSizeInBytes += version.getDirSizeInBytes();
      }
      return totalSizeInBytes;
    }

    private FileTime getLastReferenceTime(final ProjectVersion pv) throws IOException {
      final Path dirSizeFile = Paths
          .get(pv.getInstalledDir().toPath().toString(), FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME);
      return Files.getLastModifiedTime(dirSizeFile);
    }

    private void deleteLeastRecentlyUsedProjects(long sizeToFreeInBytes,
        final List<ProjectVersion>
            projectVersions) throws IOException {
      // sort project version by last reference time in ascending order
      try {
        projectVersions.sort((o1, o2) -> {
          try {
            final FileTime lastReferenceTime1 = getLastReferenceTime(o1);
            final FileTime lastReferenceTime2 = getLastReferenceTime(o2);
            return lastReferenceTime1.compareTo(lastReferenceTime2);
          } catch (final IOException ex) {
            throw new RuntimeException(ex);
          }
        });
      } catch (final RuntimeException ex) {
        throw new IOException(ex);
      }

      for (final ProjectVersion version : projectVersions) {
        if (sizeToFreeInBytes > 0) {
          try {
            // delete the project directory even if flow within is running. It's ok to
            // delete the directory since execution dir is HARD linked to project dir.
            FlowRunnerManager.deleteDirectory(version);
            FlowPreparer.this.installedProjects.remove(new Pair<>(version.getProjectId(), version
                .getVersion()));
            sizeToFreeInBytes -= version.getDirSizeInBytes();
          } catch (final IOException ex) {
            log.error(ex);
          }
        }
      }
    }

    void deleteProjectDirsIfNecessary(final long spaceToDeleteInBytes) throws IOException {
      final long currentSpaceInBytes = getProjectDirsTotalSizeInBytes();
      if (this.projectDirMaxSizeInMb != null
          && (currentSpaceInBytes + spaceToDeleteInBytes) >= this
          .projectDirMaxSizeInMb * 1024 * 1024) {
        deleteLeastRecentlyUsedProjects(spaceToDeleteInBytes,
            new ArrayList<>(FlowPreparer.this.installedProjects.values()));
      }
    }
  }
}
