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

import static azkaban.utils.ThinArchiveUtils.getDependencyFile;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import azkaban.execapp.metric.ProjectCacheHitRatio;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerException;
import azkaban.project.ProjectFileHandler;
import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import azkaban.storage.ProjectStorageManager;
import azkaban.utils.DependencyTransferException;
import azkaban.utils.DependencyTransferManager;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Utils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlowPreparer {

  // Name of the file which keeps project directory size
  static final String PROJECT_DIR_SIZE_FILE_NAME = "___azkaban_project_dir_size_in_bytes___";

  private static final Logger LOGGER = LoggerFactory.getLogger(FlowPreparer.class);

  // TODO spyne: move to config class
  private final File executionsDir;
  // TODO spyne: move to config class
  private final File projectCacheDir;
  private final ProjectStorageManager projectStorageManager;
  // Null if cache clean-up is disabled
  private final Optional<ProjectCacheCleaner> projectCacheCleaner;
  private final ProjectCacheHitRatio projectCacheHitRatio;
  private final DependencyTransferManager dependencyTransferManager;

  FlowPreparer(final ProjectStorageManager projectStorageManager, final DependencyTransferManager dependencyTransferManager,
      final File projectsDir, final ProjectCacheCleaner cleaner, final ProjectCacheHitRatio projectCacheHitRatio,
      final File executionsDir) {
    Preconditions.checkNotNull(projectStorageManager);
    Preconditions.checkNotNull(executionsDir);
    Preconditions.checkNotNull(projectsDir);
    Preconditions.checkNotNull(projectCacheHitRatio);

    Preconditions.checkArgument(projectsDir.exists());
    Preconditions.checkArgument(executionsDir.exists());

    this.projectStorageManager = projectStorageManager;
    this.executionsDir = executionsDir;
    this.projectCacheDir = projectsDir;
    this.projectCacheCleaner = Optional.ofNullable(cleaner);
    this.projectCacheHitRatio = projectCacheHitRatio;
    this.dependencyTransferManager = dependencyTransferManager;
  }

  /**
   * Constructor
   * This constructor is used for Containerized execution which does not need projectCache
   *
   * @param projectStorageManager projectStorageManager
   * @param dependencyTransferManager dependencyStorageManager
   */
  public FlowPreparer(final ProjectStorageManager projectStorageManager,
               final DependencyTransferManager dependencyTransferManager) {
    Preconditions.checkNotNull(projectStorageManager);
    Preconditions.checkNotNull(dependencyTransferManager);

    this.projectStorageManager = projectStorageManager;
    this.dependencyTransferManager = dependencyTransferManager;

    // Not needed.
    this.executionsDir = null;
    this.projectCacheDir = null;
    this.projectCacheCleaner = Optional.empty();
    this.projectCacheHitRatio = null;
  }

  /**
   * Calculate the directory size and save it to a file.
   *
   * @param dir the directory whose size needs to be saved.
   * @return the size of the dir.
   */
  static long calculateDirSizeAndSave(final File dir) throws IOException {
    final Path path = Paths.get(dir.getPath(), FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME);
    if (!Files.exists(path)) {
      final long sizeInByte = FileUtils.sizeOfDirectory(dir);
      FileIOUtils.dumpNumberToFile(path, sizeInByte);
      return sizeInByte;
    } else {
      return FileIOUtils.readNumberFromFile(path);
    }
  }


  /**
   * Prepare the flow directory for execution.
   *
   * @param flow Executable Flow instance.
   */
  void setup(final ExecutableFlow flow) throws ExecutorManagerException {
    File tempDir = null;
    try {
      final ProjectDirectoryMetadata project = new ProjectDirectoryMetadata(
          flow.getProjectId(),
          flow.getVersion());

      final long flowPrepStartTime = System.currentTimeMillis();

      tempDir = downloadProjectIfNotExists(project, flow.getExecutionId());

      LOGGER.info("Project is setup for execution {}", flow.getExecutionId());
      // With synchronization, only one thread is allowed to proceed to avoid complicated race
      // conditions which could arise when multiple threads are downloading/deleting/hard-linking
      // the same project. But it doesn't prevent multiple executor processes interfering with each
      // other triggering race conditions. So it's important to operationally make sure that only
      // one executor process is setting up flow execution against the shared project directory.
      long criticalSectionStartTime = -1;
      File execDir = null;

      synchronized (this) {
        LOGGER.info("Setting up execution dir for {}", flow.getExecutionId());
        criticalSectionStartTime = System.currentTimeMillis();
        if (!project.getInstalledDir().exists() && tempDir != null) {
          // If new project is downloaded and project dir cache clean-up feature is enabled, then
          // perform clean-up if size of all project dirs exceeds the cache size.
          if (this.projectCacheCleaner.isPresent()) {
            this.projectCacheCleaner.get()
                .deleteProjectDirsIfNecessary(project.getDirSizeInByte());
          }
          // Rename temp dir to a proper project directory name.
          Files.move(tempDir.toPath(), project.getInstalledDir().toPath());
        }

        final long start = System.currentTimeMillis();
        execDir = setupExecutionDir(project.getInstalledDir(), flow);
        final long end = System.currentTimeMillis();
        LOGGER.info("Setting up execution dir {} took {} sec(s)", execDir, (end - start) / 1000);
      }

      final long flowPrepCompletionTime = System.currentTimeMillis();
      LOGGER.info("Flow preparation completed in {} sec(s), out of which {} sec(s) was spent inside "
              + "critical section. [execid: {}, path: {}]",
          (flowPrepCompletionTime - flowPrepStartTime) / 1000,
          (flowPrepCompletionTime - criticalSectionStartTime) / 1000,
          flow.getExecutionId(), execDir.getPath());
    } catch (final Exception ex) {
      FileIOUtils.deleteDirectorySilently(tempDir);
      LOGGER.error("Error in preparing flow execution {}", flow.getExecutionId(), ex);
      throw new ExecutorManagerException(ex);
    }
  }

  public void setupContainerizedExecution(final ExecutableFlow flow, final File projectDir)
    throws ExecutorManagerException {
    final ProjectDirectoryMetadata projectDirMetadata = new ProjectDirectoryMetadata(
              flow.getProjectId(),
              flow.getVersion());
    final long flowPrepStartTime = System.currentTimeMillis();
    final int execId = flow.getExecutionId();

    LOGGER.info("Using containerized execution flow setup");
    try {
      downloadAndUnzipProject(projectDirMetadata, execId, projectDir);
      final long flowPrepCompletionTime = System.currentTimeMillis();
      LOGGER.info("Flow preparation completed in {} sec(s). [execid: {}, path: {}]",
              (flowPrepCompletionTime - flowPrepStartTime) / 1000,
              flow.getExecutionId(), projectDir.getPath());
    } catch (final IOException e) {
      LOGGER.error("Error in downloading project files for flow execution {}", execId, e);
      throw new ExecutorManagerException(e);
    }
  }

  private File setupExecutionDir(final File installedDir, final ExecutableFlow flow)
      throws IOException {
    File execDir = null;
    try {
      execDir = createExecDir(flow);
      // Create hardlinks from the project
      FileIOUtils.createDeepHardlink(installedDir, execDir);
      return execDir;
    } catch (final Exception ex) {
      FileIOUtils.deleteDirectorySilently(execDir);
      throw ex;
    }
  }

  /**
   * Update last modified time of the file if it exists.
   *
   * @param path path to the target file
   */
  @VisibleForTesting
  void updateLastModifiedTime(final Path path) {
    try {
      Files.setLastModifiedTime(path, FileTime.fromMillis(System.currentTimeMillis()));
    } catch (final IOException ex) {
      LOGGER.warn("Error when updating last modified time for {}", path, ex);
    }
  }

  /**
   * @return the project directory name of a project
   */
  private String generateProjectDirName(final ProjectDirectoryMetadata proj) {
    return String.valueOf(proj.getProjectId()) + "." + String.valueOf(proj.getVersion());
  }

  private File createTempDir(final ProjectDirectoryMetadata proj) {
    final String projectDir = generateProjectDirName(proj);
    final File tempDir = new File(this.projectCacheDir,
        "_temp." + projectDir + "." + System.currentTimeMillis());
    tempDir.mkdirs();
    return tempDir;
  }

  @VisibleForTesting
  void downloadAndUnzipProject(final ProjectDirectoryMetadata proj, final int execId, final File dest)
      throws IOException {
    final long start = System.currentTimeMillis();
    final ProjectFileHandler projectFileHandler = requireNonNull(this.projectStorageManager
        .getProjectFile(proj.getProjectId(), proj.getVersion()));
    LOGGER.info("Downloading zip file for project {} when preparing "
            + "execution [execid {}] completed in {} second(s)", proj, execId,
        (System.currentTimeMillis() - start) / 1000);

    try {
      checkState("zip".equalsIgnoreCase(projectFileHandler.getFileType()));
      final File zipFile = requireNonNull(projectFileHandler.getLocalFile());
      final ZipFile zip = new ZipFile(zipFile);
      Utils.unzip(zip, dest);

      // Download all startup dependencies. If this is a fat archive, it will be an empty set (so we won't download
      // anything). Note that we are getting our list of startup dependencies from the DB, NOT from the
      // startup-dependencies.json file contained in the archive. Both should be IDENTICAL, however we chose to get the
      // list from the DB because this will be consistent with how containerized executions determine the startup
      // dependency list.
      downloadAllDependencies(proj, execId, dest, projectFileHandler.getStartupDependencies());

      proj.setDirSizeInByte(calculateDirSizeAndSave(dest));
    } finally {
      projectFileHandler.deleteLocalFile();
    }
  }

  /**
   * Download necessary JAR dependencies from storage
   *
   * @param proj project to download
   * @param execId execution id number
   * @param folder root of unzipped project
   * @param dependencies the set of dependencies to download
   */
  private void downloadAllDependencies(final ProjectDirectoryMetadata proj, final int execId, final File folder,
      final Set<Dependency> dependencies) {
    // Download all of the dependencies from storage
    LOGGER.info("Downloading {} JAR dependencies... Project: {}, ExecId: {}", dependencies.size(), proj, execId);
    Set<DependencyFile> depFiles = dependencies
        .stream()
        .map(d -> getDependencyFile(folder, d))
        .collect(Collectors.toSet());

    try {
      final long start = System.currentTimeMillis();
      this.dependencyTransferManager.downloadAllDependencies(depFiles);
      LOGGER.info("Downloading {} JAR dependencies for project {} when preparing "
              + "execution [execid {}] completed in {} second(s)", dependencies.size(), proj, execId,
          (System.currentTimeMillis() - start) / 1000);
    } catch (DependencyTransferException e) {
      LOGGER.error("Unable to download one or more dependencies when preparing execId {}.",
          execId, proj);
      throw e;
    }
  }

  /**
   * Download project zip and unzip it if not exists locally.
   *
   * @param proj project to download
   * @param execId execution id number
   * @return the temp dir where the new project is downloaded to, null if no project is downloaded.
   * @throws IOException if downloading or unzipping fails.
   */
  @VisibleForTesting
  File downloadProjectIfNotExists(final ProjectDirectoryMetadata proj, final int execId)
      throws IOException {
    final String projectDir = generateProjectDirName(proj);
    if (proj.getInstalledDir() == null) {
      proj.setInstalledDir(new File(this.projectCacheDir, projectDir));
    }

    // If directory exists, assume it's prepared and skip.
    if (proj.getInstalledDir().exists()) {
      LOGGER.info("Project {} already cached. Skipping download. ExecId: {}", proj, execId);
      // Hit the local cache.
      this.projectCacheHitRatio.markHit();
      // Update last modified time of the file keeping project dir size when the project is
      // accessed. This last modified time will be used to determined least recently used
      // projects when performing project directory clean-up.
      updateLastModifiedTime(
          Paths.get(proj.getInstalledDir().getPath(), PROJECT_DIR_SIZE_FILE_NAME));
      return null;
    }

    this.projectCacheHitRatio.markMiss();

    // Download project to a temp dir if not exists in local cache.
    final File tempDir = createTempDir(proj);
    downloadAndUnzipProject(proj, execId, tempDir);

    return tempDir;
  }

  private File createExecDir(final ExecutableFlow flow) {
    final int execId = flow.getExecutionId();
    final File execDir = new File(this.executionsDir, String.valueOf(execId));
    flow.setExecutionPath(execDir.getPath());
    execDir.mkdirs();
    return execDir;
  }

  public void shutdown() {
    if (projectCacheCleaner.isPresent()) {
      this.projectCacheCleaner.get().shutdown();
    }
  }
}
