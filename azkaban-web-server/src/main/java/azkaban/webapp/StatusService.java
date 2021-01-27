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

package azkaban.webapp;

import static azkaban.webapp.servlet.AbstractAzkabanServlet.jarVersion;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.DispatchMethod;
import azkaban.db.DatabaseOperator;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.imagemgmt.dto.ImageVersionMetadataResponseDTO;
import azkaban.imagemgmt.services.ImageVersionMetadataService;
import azkaban.utils.Props;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class StatusService {

  private static final Logger log = LoggerFactory.getLogger(StatusService.class);
  private static final File PACKAGE_JAR = new File(
      StatusService.class.getProtectionDomain().getCodeSource().getLocation().getPath());
  private final ExecutorLoader executorLoader;
  private final DatabaseOperator dbOperator;
  private final String pidFilename;
  private final Props props;
  private final ImageVersionMetadataService versionMetadataService;

  @Inject
  public StatusService(final Props props, final ExecutorLoader executorLoader,
      final DatabaseOperator dbOperator, final ImageVersionMetadataService versionMetadataService) {
    this.executorLoader = executorLoader;
    this.dbOperator = dbOperator;
    this.props = props;
    this.versionMetadataService = versionMetadataService;
    this.pidFilename = props.getString(ConfigurationKeys.AZKABAN_PID_FILENAME, "currentpid");
  }

  private static String getInstallationPath() {
    try {
      return PACKAGE_JAR.getCanonicalPath();
    } catch (final IOException e) {
      log.error("Unable to obtain canonical path. Reporting absolute path instead", e);
      return PACKAGE_JAR.getAbsolutePath();
    }
  }

  /**
   * Gets the status of the azkaban cluster.
   * @return Status
   */
  public Status getStatus() {
    if(isContainerizedDispatchMethodEnabled()) {
      return getContainerizedStatus();
    }
    return getClusterStatus();
  }

  /**
   * This returns implementation instance for Status containing status information for Azkaban
   * cluster pertaining to web server, memory, active executors etc.
   * @return Status
   */
  private Status getClusterStatus() {
    final String version = jarVersion == null ? "unknown" : jarVersion;
    final Runtime runtime = Runtime.getRuntime();
    final long usedMemory = runtime.totalMemory() - runtime.freeMemory();

    // Build the status object
    return new ClusterStatus(version,
        getPid(),
        getInstallationPath(),
        usedMemory,
        runtime.maxMemory(),
        getDbStatus(),
        getActiveExecutors());
  }

  /**
   * This returns implementation instance for Status containing cluster status information for
   * Azkaban containerization. It includes information pertaining to web server, memory and
   * containerization information such as image types and version for each image types selected
   * based on either random rampup or latest active image version.
   * @return Status
   */
  private Status getContainerizedStatus() {
    final String version = jarVersion == null ? "unknown" : jarVersion;
    final Runtime runtime = Runtime.getRuntime();
    final long usedMemory = runtime.totalMemory() - runtime.freeMemory();
    Map<String, ImageVersionMetadataResponseDTO> imageTypeVersionMap = new TreeMap<>();
    try {
      imageTypeVersionMap = this.versionMetadataService.getVersionMetadataForAllImageTypes();
    } catch (final Exception ex) {
      log.error("Error while geting version metadata for all the image types", ex);
    }
    // Build the status object
    return new ContainerizedClusterStatus(version,
        getPid(),
        getInstallationPath(),
        usedMemory,
        runtime.maxMemory(),
        getDbStatus(),
        imageTypeVersionMap);
  }

  private String getPid() {
    final File libDir = PACKAGE_JAR.getParentFile();
    final File installDir = libDir.getParentFile();
    final File pidFile = new File(installDir, this.pidFilename);
    try {
      return Files.readFirstLine(pidFile, StandardCharsets.UTF_8).trim();
    } catch (final IOException e) {
      log.error("Unable to obtain PID", e);
      return "unknown";
    }
  }

  private Map<Integer, Executor> getActiveExecutors() {
    final Map<Integer, Executor> executorMap = new HashMap<>();
    try {
      final List<Executor> executors = this.executorLoader.fetchActiveExecutors();
      for (final Executor executor : executors) {
        executorMap.put(executor.getId(), executor);
      }
    } catch (final ExecutorManagerException e) {
      log.error("Fetching executors failed!", e);
    }
    return executorMap;
  }

  private boolean getDbStatus() {
    try {
      return this.dbOperator.query("SELECT 1", rs -> true);
    } catch (final SQLException e) {
      log.error("DB Error", e);
    }
    return false;
  }

  private boolean isContainerizedDispatchMethodEnabled() {
    return DispatchMethod.isContainerizedMethodEnabled(props
        .getString(Constants.ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD,
            DispatchMethod.PUSH.name()));
  }

}
