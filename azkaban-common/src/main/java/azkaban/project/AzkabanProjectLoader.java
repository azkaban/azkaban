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

package azkaban.project;

import static azkaban.utils.ThinArchiveUtils.*;
import static java.util.Objects.requireNonNull;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.db.DatabaseOperator;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionReference;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.metrics.CommonMetrics;
import azkaban.project.FlowLoaderUtils.DirFilter;
import azkaban.project.FlowLoaderUtils.SuffixFilter;
import azkaban.project.ProjectLogEvent.EventType;
import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidationStatus;
import azkaban.spi.Storage;
import azkaban.storage.ProjectStorageManager;
import azkaban.user.User;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import azkaban.utils.ValidatorUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;
import javax.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the downloading and uploading of projects.
 */
class AzkabanProjectLoader {

  private static final Logger log = LoggerFactory.getLogger(AzkabanProjectLoader.class);
  private static final String DIRECTORY_FLOW_REPORT_KEY = "Directory Flow";

  private final Props props;

  private final CommonMetrics commonMetrics;
  private final ProjectLoader projectLoader;
  private final ProjectStorageManager projectStorageManager;
  private final FlowLoaderFactory flowLoaderFactory;
  private final DatabaseOperator dbOperator;
  private final ArchiveUnthinner archiveUnthinner;
  private final File tempDir;
  private final int projectVersionRetention;
  private final ExecutorLoader executorLoader;
  private final Storage storage;
  private final ValidatorUtils validatorUtils;

  @Inject
  AzkabanProjectLoader(final Props props, final CommonMetrics commonMetrics, final ProjectLoader projectLoader,
      final ProjectStorageManager projectStorageManager, final FlowLoaderFactory flowLoaderFactory,
      final ExecutorLoader executorLoader, final DatabaseOperator databaseOperator,
      final Storage storage, final ArchiveUnthinner archiveUnthinner,
      final ValidatorUtils validatorUtils) {
    this.props = requireNonNull(props, "Props is null");
    this.projectLoader = requireNonNull(projectLoader, "project Loader is null");
    this.projectStorageManager = requireNonNull(projectStorageManager, "Storage Manager is null");
    this.flowLoaderFactory = requireNonNull(flowLoaderFactory, "Flow Loader Factory is null");

    this.commonMetrics = commonMetrics;
    this.dbOperator = databaseOperator;
    this.storage = storage;
    this.archiveUnthinner = archiveUnthinner;
    this.validatorUtils = validatorUtils;

    this.tempDir = new File(props.getString(ConfigurationKeys.PROJECT_TEMP_DIR, "temp"));
    this.executorLoader = executorLoader;
    if (!this.tempDir.exists()) {
      log.info("Creating temp dir: " + this.tempDir.getAbsolutePath());
      this.tempDir.mkdirs();
    } else {
      log.info("Using temp dir: " + this.tempDir.getAbsolutePath());
    }
    this.projectVersionRetention = props.getInt(ConfigurationKeys.PROJECT_VERSION_RETENTION, 3);
    log.info("Project version retention is set to " + this.projectVersionRetention);
  }

  public Map<String, ValidationReport> uploadProject(final Project project,
      final File archive, final String fileType, final User uploader, final Props additionalProps)
      throws ProjectManagerException, ExecutorManagerException {
    log.info("Uploading files to " + project.getName());
    final Map<String, ValidationReport> reports;

    File folder = null;
    final FlowLoader loader;

    try {
      folder = unzipProject(archive, fileType);

      File startupDependencies = getStartupDependenciesFile(folder);
      reports = startupDependencies.exists()
          ? this.archiveUnthinner.validateProjectAndPersistDependencies(project, folder,
            startupDependencies, additionalProps)
          : this.validatorUtils.validateProject(project, folder, additionalProps);

      // If any files in the project folder have been modified or removed, update the project zip
      if (reports.values().stream().anyMatch(r -> !r.getModifiedFiles().isEmpty() || !r.getRemovedFiles().isEmpty())) {
        updateProjectZip(archive, folder);
      }

      loader = this.flowLoaderFactory.createFlowLoader(folder);
      reports.put(DIRECTORY_FLOW_REPORT_KEY, loader.loadProjectFlow(project, folder));

      // Check the validation report.
      if (!isReportStatusValid(reports, project)) {
        FlowLoaderUtils.cleanUpDir(folder);
        return reports;
      }

      // Upload the project to DB and storage.
      File startupDependenciesOrNull = startupDependencies.exists() ? startupDependencies : null;
      persistProject(project, loader, archive, folder, startupDependenciesOrNull, uploader);

      if (startupDependencies.exists()) {
        // Mark that we uploaded a thin zip in the metrics.
        commonMetrics.markUploadThinProject();
      } else {
        commonMetrics.markUploadFatProject();
      }

    } finally {
      FlowLoaderUtils.cleanUpDir(folder);
    }

    // Clean up project old installations after new project is uploaded successfully.
    cleanUpProjectOldInstallations(project);

    return reports;
  }

  private void updateProjectZip(final File zipFile, final File folder) {
    try {
      File newZipFile = new File(zipFile.getAbsolutePath().concat(".byte-ray.new"));
      Utils.zipFolderContent(folder, newZipFile);
      FileUtils.deleteQuietly(zipFile);
      FileUtils.moveFile(newZipFile, zipFile);
    } catch (IOException e) {
      folder.deleteOnExit();
      throw new ProjectManagerException("Error when generating the modified zip.", e);
    }
  }

  private File unzipProject(final File archive, final String fileType)
      throws ProjectManagerException {
    final File file;
    try {
      if (fileType == null) {
        throw new ProjectManagerException("Unknown file type for "
            + archive.getName());
      } else if ("zip".equals(fileType)) {
        file = unzipFile(archive);
      } else {
        throw new ProjectManagerException("Unsupported archive type for file "
            + archive.getName());
      }
    } catch (final IOException e) {
      throw new ProjectManagerException("Error unzipping file.", e);
    }
    return file;
  }

  private boolean isReportStatusValid(final Map<String, ValidationReport> reports,
      final Project project) {
    ValidationStatus status = ValidationStatus.PASS;
    for (final Entry<String, ValidationReport> report : reports.entrySet()) {
      if (report.getValue().getStatus().compareTo(status) > 0) {
        status = report.getValue().getStatus();
      }
    }
    if (status == ValidationStatus.ERROR) {
      log.error("Error found in uploading to " + project.getName());
      return false;
    }
    return true;
  }

  private void persistProject(final Project project, final FlowLoader loader, final File archive,
      final File projectDir, final File startupDependencies, final User uploader) throws ProjectManagerException {
    synchronized (project) {
      final int newProjectVersion = this.projectLoader.getLatestProjectVersion(project) + 1;
      final Map<String, Flow> flows = loader.getFlowMap();
      for (final Flow flow : flows.values()) {
        flow.setProjectId(project.getId());
        flow.setVersion(newProjectVersion);
      }

      this.projectStorageManager.uploadProject(project, newProjectVersion, archive, startupDependencies, uploader);

      log.info("Uploading flow to db for project " + archive.getName());
      this.projectLoader.uploadFlows(project, newProjectVersion, flows.values());
      log.info("Changing project versions for project " + archive.getName());
      this.projectLoader.changeProjectVersion(project, newProjectVersion,
          uploader.getUserId());
      project.setFlows(flows);

      if (loader instanceof DirectoryFlowLoader) {
        final DirectoryFlowLoader directoryFlowLoader = (DirectoryFlowLoader) loader;
        log.info("Uploading Job properties");
        this.projectLoader.uploadProjectProperties(project, new ArrayList<>(
            directoryFlowLoader.getJobPropsMap().values()));
        log.info("Uploading Props properties");
        this.projectLoader.uploadProjectProperties(project, directoryFlowLoader.getPropsList());

      } else if (loader instanceof DirectoryYamlFlowLoader) {
        uploadFlowFilesRecursively(projectDir, project, newProjectVersion);
      } else {
        throw new ProjectManagerException("Invalid type of flow loader.");
      }

      this.projectLoader.postEvent(project, EventType.UPLOADED, uploader.getUserId(),
          "Uploaded project files zip " + archive.getName());
    }
  }

  private void uploadFlowFilesRecursively(final File projectDir, final Project project, final int
      newProjectVersion) {
    for (final File file : projectDir.listFiles(new SuffixFilter(Constants.FLOW_FILE_SUFFIX))) {
      final int newFlowVersion = this.projectLoader
          .getLatestFlowVersion(project.getId(), newProjectVersion, file.getName()) + 1;
      this.projectLoader
          .uploadFlowFile(project.getId(), newProjectVersion, file, newFlowVersion);
    }
    for (final File file : projectDir.listFiles(new DirFilter())) {
      uploadFlowFilesRecursively(file, project, newProjectVersion);
    }
  }

  private void cleanUpProjectOldInstallations(final Project project)
      throws ProjectManagerException, ExecutorManagerException {
    log.info("Cleaning up old install files older than "
        + (project.getVersion() - this.projectVersionRetention));
    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> unfinishedFlows = this.executorLoader
        .fetchUnfinishedFlowsMetadata();
    final List<Integer> versionsWithUnfinishedExecutions = unfinishedFlows.values()
        .stream().map(pair -> pair.getSecond())
        .filter(exflow -> exflow.getProjectId() == project.getId())
        .map(exflow -> exflow.getVersion())
        .collect(Collectors.toList());
    this.projectLoader.cleanOlderProjectVersion(project.getId(),
        project.getVersion() - this.projectVersionRetention, versionsWithUnfinishedExecutions);
    // Clean up storage
    this.projectStorageManager.cleanupProjectArtifacts(project.getId(), versionsWithUnfinishedExecutions);
  }

  private File unzipFile(final File archiveFile) throws IOException {
    final ZipFile zipfile = new ZipFile(archiveFile);
    final File unzipped = Utils.createTempDir(this.tempDir);
    Utils.unzip(zipfile, unzipped);
    zipfile.close();

    return unzipped;
  }

  public ProjectFileHandler getProjectFile(final Project project, int version)
      throws ProjectManagerException {
    if (version == -1) {
      version = this.projectLoader.getLatestProjectVersion(project);
    }
    return this.projectStorageManager.getProjectFile(project.getId(), version);
  }

}
