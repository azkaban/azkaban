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
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Pair;
import azkaban.utils.Utils;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class FlowPreparer {
  private static final Logger logger = Logger.getLogger(FlowPreparer.class);

  private final ProjectLoader projectLoader;
  private final File executionsDir;
  private final File projectsDir;
  private final Map<Pair<Integer, Integer>, ProjectVersion> installedProjects;

  public FlowPreparer(ProjectLoader projectLoader,
      File executionsDir,
      File projectsDir,
      Map<Pair<Integer, Integer>, ProjectVersion> installedProjects) {
    this.projectLoader = projectLoader;
    this.executionsDir = executionsDir;
    this.projectsDir = projectsDir;
    this.installedProjects = installedProjects;
  }

  void setup(ExecutableFlow flow) {
    File execDir = null;
    try {
      // First get the ProjectVersion
      final ProjectVersion projectVersion = getProjectVersion(flow);

      // Setup the project
      setupProjectFiles(projectVersion);

      // Create the execution directory
      execDir = createExecDir(flow);

      // Create the symlinks from the project
      copyCreateHardlinkDirectory(projectVersion.getInstalledDir(), execDir);
    } catch (Exception e) {
      logger.error("Error in setting up project directory: " + projectsDir + ", Exception: " + e);
      if (execDir != null) {
        cleanup(execDir);
      }
      Throwables.propagate(e);
    }
  }

  public synchronized void setupProjectFiles(final ProjectVersion pv)
      throws ProjectManagerException, IOException {
    final int projectId = pv.getProjectId();
    final int version = pv.getVersion();

    final String projectDir = String.valueOf(projectId) + "." + String.valueOf(version);
    if (pv.getInstalledDir() == null) {
      pv.setInstalledDir(new File(projectsDir, projectDir));
    }

    // If directory exists. Assume its prepared and skip.
    if (pv.getInstalledDir().exists()) {
      return;
    }

    logger.info("Preparing Project: " + pv);

    File tempDir = new File(projectsDir, "_temp." + projectDir + "." + System.currentTimeMillis());
    tempDir.mkdirs();

    ProjectFileHandler projectFileHandler = null;
    try {
      projectFileHandler = projectLoader.getUploadedFile(projectId, version);
      Preconditions.checkState("zip".equals(projectFileHandler.getFileType()));

      logger.info("Downloading zip file.");
      ZipFile zip = new ZipFile(projectFileHandler.getLocalFile());
      Utils.unzip(zip, tempDir);

      Files.move(tempDir.toPath(), pv.getInstalledDir().toPath(), StandardCopyOption.ATOMIC_MOVE);
    } finally {

      if (projectFileHandler != null) {
        projectFileHandler.deleteLocalFile();
      }

      // Clean up: Remove tempDir if exists
      if (tempDir.exists()) {
        FileUtils.deleteDirectory(tempDir);
      }
    }
  }

  private void copyCreateHardlinkDirectory(File projectDir, File execDir) throws IOException {
    FileIOUtils.createDeepHardlink(projectDir, execDir);
  }

  private File createExecDir(ExecutableFlow flow) {
    final int execId = flow.getExecutionId();
    File execDir = new File(executionsDir, String.valueOf(execId));
    flow.setExecutionPath(execDir.getPath());

    logger.info("Flow " + execId + " submitted with path " + execDir.getPath());
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
    if (execDir.exists()) {
      try {
        FileUtils.deleteDirectory(execDir);
      } catch (IOException e) {
        Throwables.propagate(e);
      }
    }
  }
}
