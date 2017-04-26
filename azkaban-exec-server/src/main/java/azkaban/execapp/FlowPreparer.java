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
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import static com.google.common.base.Preconditions.*;
import static java.util.Objects.*;


public class FlowPreparer {
  private static final Logger log = Logger.getLogger(FlowPreparer.class);

  // TODO spyne: move to config class
  private final File executionsDir;
  // TODO spyne: move to config class
  private final File projectsDir;

  private final Map<Pair<Integer, Integer>, ProjectVersion> installedProjects;
  private final StorageManager storageManager;

  public FlowPreparer(StorageManager storageManager, File executionsDir, File projectsDir,
      Map<Pair<Integer, Integer>, ProjectVersion> installedProjects) {
    this.storageManager = storageManager;
    this.executionsDir = executionsDir;
    this.projectsDir = projectsDir;
    this.installedProjects = installedProjects;
  }

  /**
   * Prepare the flow directory for execution.
   *
   * @param flow Executable Flow instance.
   */
  void setup(ExecutableFlow flow) {
    File execDir = null;
    try {
      // First get the ProjectVersion
      final ProjectVersion projectVersion = getProjectVersion(flow);

      // Setup the project
      setupProject(projectVersion);

      // Create the execution directory
      execDir = createExecDir(flow);

      // Create the symlinks from the project
      copyCreateHardlinkDirectory(projectVersion.getInstalledDir(), execDir);

      log.info(String.format("Flow Preparation complete. [execid: %d, path: %s]",
          flow.getExecutionId(), execDir.getPath()));
    } catch (Exception e) {
      log.error("Error in setting up project directory: " + projectsDir + ", Exception: " + e);
      cleanup(execDir);
      throw new RuntimeException(e);
    }
  }

  /**
   * Prepare the project directory.
   *
   * @param pv ProjectVersion object
   * @throws ProjectManagerException
   * @throws IOException
   */
  @VisibleForTesting
  void setupProject(final ProjectVersion pv)
      throws ProjectManagerException, IOException {
    final int projectId = pv.getProjectId();
    final int version = pv.getVersion();

    final String projectDir = String.valueOf(projectId) + "." + String.valueOf(version);
    if (pv.getInstalledDir() == null) {
      pv.setInstalledDir(new File(projectsDir, projectDir));
    }

    // If directory exists. Assume its prepared and skip.
    if (pv.getInstalledDir().exists()) {
      log.info("Project already cached. Skipping download. " + pv);
      return;
    }

    log.info("Preparing Project: " + pv);

    File tempDir = new File(projectsDir, "_temp." + projectDir + "." + System.currentTimeMillis());

    // TODO spyne: Why mkdirs? This path should be already set up.
    tempDir.mkdirs();

    ProjectFileHandler projectFileHandler = null;
    try {
      projectFileHandler = requireNonNull(storageManager.getProjectFile(projectId, version));
      checkState("zip".equals(projectFileHandler.getFileType()));

      log.info("Downloading zip file.");
      final File zipFile = requireNonNull(projectFileHandler.getLocalFile());
      final ZipFile zip = new ZipFile(zipFile);
      Utils.unzip(zip, tempDir);

      Files.move(tempDir.toPath(), pv.getInstalledDir().toPath(), StandardCopyOption.ATOMIC_MOVE);

      log.warn(String.format("Project Preparation complete. [%s]", pv));
    } finally {

      if (projectFileHandler != null) {
        projectFileHandler.deleteLocalFile();
      }

      // Clean up: Remove tempDir if exists
      FileUtils.deleteDirectory(tempDir);
    }
  }

  private void copyCreateHardlinkDirectory(File projectDir, File execDir) throws IOException {
    FileIOUtils.createDeepHardlink(projectDir, execDir);
  }

  private File createExecDir(ExecutableFlow flow) {
    final int execId = flow.getExecutionId();
    File execDir = new File(executionsDir, String.valueOf(execId));
    flow.setExecutionPath(execDir.getPath());

    // TODO spyne: Why mkdirs? This path should be already set up.
    execDir.mkdirs();
    return execDir;
  }

  private ProjectVersion getProjectVersion(ExecutableFlow flow) {
    // We're setting up the installed projects. First time, it may take a while
    // to set up.
    final ProjectVersion projectVersion;
    synchronized (installedProjects) {
      projectVersion = installedProjects.computeIfAbsent(new Pair<>(flow.getProjectId(), flow.getVersion()),
          k -> new ProjectVersion(flow.getProjectId(), flow.getVersion()));
    }
    return projectVersion;
  }

  private void cleanup(File execDir) {
    if (execDir != null) {
      try {
        FileUtils.deleteDirectory(execDir);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
