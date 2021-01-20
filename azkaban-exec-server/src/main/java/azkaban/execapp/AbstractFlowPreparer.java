/*
 * Copyright 2021 LinkedIn Corp.
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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static azkaban.utils.ThinArchiveUtils.getDependencyFile;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * This class is used as abstract class for all FlowPreparer implementations.
 */
public abstract class AbstractFlowPreparer {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFlowPreparer.class);
  protected final ProjectStorageManager projectStorageManager;
  protected final DependencyTransferManager dependencyTransferManager;
  // Name of the file which keeps project directory size
  public static final String PROJECT_DIR_SIZE_FILE_NAME = "___azkaban_project_dir_size_in_bytes___";

  /**
   * Constructor
   *
   * @param projectStorageManager projectStorageManager
   * @param dependencyTransferManager dependencyStorageManager
   */
  public AbstractFlowPreparer(final ProjectStorageManager projectStorageManager,
                      final DependencyTransferManager dependencyTransferManager) {
    Preconditions.checkNotNull(projectStorageManager);
    Preconditions.checkNotNull(dependencyTransferManager);

    this.projectStorageManager = projectStorageManager;
    this.dependencyTransferManager = dependencyTransferManager;
  }

  /**
   * This method must be implemented by respective FlowPreparer class.
   * @param flow the executable flow
   * @throws ExecutorManagerException
   */
  public abstract void setup(final ExecutableFlow flow) throws ExecutorManagerException;

  /**
   *
   * @param dir Directory to be used by respective FlowPreparer to setup Execution dir
   * @param flow executable flow.
   * @return File object of execution directory.
   * @throws ExecutorManagerException
   */
  protected abstract File setupExecutionDir(final Path dir, final ExecutableFlow flow)
    throws ExecutorManagerException;


  /**
   * Calculate the directory size and save it to a file.
   *
   * @param dir the directory whose size needs to be saved.
   * @return the size of the dir.
   */
  static long calculateDirSizeAndSave(final File dir) throws IOException {
    final Path path = Paths.get(dir.getPath(), AbstractFlowPreparer.PROJECT_DIR_SIZE_FILE_NAME);
    if (!Files.exists(path)) {
      final long sizeInByte = FileUtils.sizeOfDirectory(dir);
      FileIOUtils.dumpNumberToFile(path, sizeInByte);
      return sizeInByte;
    } else {
      return FileIOUtils.readNumberFromFile(path);
    }
  }


  @VisibleForTesting
  public void downloadAndUnzipProject(final ProjectDirectoryMetadata proj, final int execId,
      final File dest) throws IOException {
    final long start = System.currentTimeMillis();
    final ProjectFileHandler projectFileHandler = requireNonNull(this.projectStorageManager
            .getProjectFile(proj.getProjectId(), proj.getVersion()));
    LOGGER.info("Downloading zip file for project {} when preparing execution [execid {}] " +
            "completed in {} second(s)", proj, execId, (System.currentTimeMillis() - start) / 1000);

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
  private void downloadAllDependencies(final ProjectDirectoryMetadata proj, final int execId,
      final File folder, final Set<Dependency> dependencies) {
    // Download all of the dependencies from storage
    LOGGER.info("Downloading {} JAR dependencies... Project: {}, ExecId: {}",
            dependencies.size(), proj, execId);
    final Set<DependencyFile> depFiles = dependencies
            .stream()
            .map(d -> getDependencyFile(folder, d))
            .collect(Collectors.toSet());

    try {
      final long start = System.currentTimeMillis();
      this.dependencyTransferManager.downloadAllDependencies(depFiles);
      LOGGER.info("Downloading {} JAR dependencies for project {} when preparing "
                      + "execution [execid {}] completed in {} second(s)",
              dependencies.size(), proj, execId, (System.currentTimeMillis() - start) / 1000);
    } catch (final DependencyTransferException e) {
      LOGGER.error("Unable to download one or more dependencies when preparing execId {} for " +
              "project {}.", execId, proj);
      throw e;
    }
  }

}
