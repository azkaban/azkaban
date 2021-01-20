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

package azkaban.container;

import azkaban.Constants;
import azkaban.execapp.AbstractFlowPreparer;
import azkaban.execapp.ProjectDirectoryMetadata;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerException;
import azkaban.storage.ProjectStorageManager;
import azkaban.utils.DependencyTransferManager;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * FlowPreparer implementation for containerized execution.
 */
public class ContainerizedFlowPreparer extends AbstractFlowPreparer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerizedFlowPreparer.class);
  @VisibleForTesting
  static final String PROJECT_DIR = "project";

  public static Path getCurrentDir() {
    // AZ_HOME must provide a correct path, if not then azHome is set to current working dir.
    final String azHome = Optional.ofNullable(System.getenv(Constants.AZ_HOME)).orElse("");
    return Paths.get(azHome).toAbsolutePath();
  }

  /**
   * Constructor
   * @param projectStorageManager ProjectStorageManager
   * @param dependencyTransferManager DependencyTransferManager
   */
  ContainerizedFlowPreparer(final ProjectStorageManager projectStorageManager,
                            final DependencyTransferManager dependencyTransferManager) {
    super(projectStorageManager, dependencyTransferManager);
  }

  /**
   * Prepare the directory for flow execution.
   *
   * @param flow Executable Flow instance.
   */
  @Override
  public void setup(final ExecutableFlow flow) throws ExecutorManagerException {
    final ProjectDirectoryMetadata projectDirMetadata = new ProjectDirectoryMetadata(
            flow.getProjectId(),
            flow.getVersion());
    final long flowPrepStartTime = System.currentTimeMillis();
    final int execId = flow.getExecutionId();

    // Setup project/execution directory
    final File projectDir = setupExecutionDir(getCurrentDir(), flow);

    LOGGER.info("Starting the flow preparation for containerized execution");
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

  @Override
  protected File setupExecutionDir(final Path dir, final ExecutableFlow flow)
          throws ExecutorManagerException {
    // Create project dir
    final Path projectDirPath = Paths.get(dir.toString(), PROJECT_DIR);
    // A directory of name projectDirPath cannot pre-exist.
    if (Files.exists(projectDirPath)) {
      final String msg = "A project directory of name " + projectDirPath + " already exists";
      LOGGER.error(msg);
      throw new ExecutorManagerException(msg);
    }

    LOGGER.info("Creating execution dir");
    try {
      Files.createDirectory(projectDirPath);
    } catch (final IOException e) {
      LOGGER.error("Error creating directory :" + projectDirPath, e);
      throw new ExecutorManagerException(e);
    }
    flow.setExecutionPath(projectDirPath.toString());
    return projectDirPath.toFile();
  }

}
