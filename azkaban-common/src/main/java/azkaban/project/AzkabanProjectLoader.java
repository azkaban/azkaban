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

import static java.util.Objects.requireNonNull;

import azkaban.Constants.ConfigurationKeys;
import azkaban.flow.Flow;
import azkaban.project.ProjectLogEvent.EventType;
import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidationStatus;
import azkaban.project.validator.ValidatorConfigs;
import azkaban.project.validator.ValidatorManager;
import azkaban.project.validator.XmlValidatorManager;
import azkaban.storage.StorageManager;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
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

  private final ProjectLoader projectLoader;
  private final StorageManager storageManager;
  private final FlowLoaderFactory flowLoaderFactory;
  private final File tempDir;
  private final int projectVersionRetention;

  @Inject
  AzkabanProjectLoader(final Props props, final ProjectLoader projectLoader,
      final StorageManager storageManager, final FlowLoaderFactory flowLoaderFactory) {
    this.props = requireNonNull(props, "Props is null");
    this.projectLoader = requireNonNull(projectLoader, "project Loader is null");
    this.storageManager = requireNonNull(storageManager, "Storage Manager is null");
    this.flowLoaderFactory = requireNonNull(flowLoaderFactory, "Flow Loader Factory is null");

    this.tempDir = new File(props.getString(ConfigurationKeys.PROJECT_TEMP_DIR, "temp"));
    if (!this.tempDir.exists()) {
      log.info("Creating temp dir: " + this.tempDir.getAbsolutePath());
      this.tempDir.mkdirs();
    } else {
      log.info("Using temp dir: " + this.tempDir.getAbsolutePath());
    }
    this.projectVersionRetention = props.getInt("project.version.retention", 3);
    log.info("Project version retention is set to " + this.projectVersionRetention);
  }

  public Map<String, ValidationReport> uploadProject(final Project project,
      final File archive, final String fileType, final User uploader, final Props additionalProps)
      throws ProjectManagerException {
    log.info("Uploading files to " + project.getName());
    final Map<String, ValidationReport> reports;

    // Since props is an instance variable of ProjectManager, and each
    // invocation to the uploadProject manager needs to pass a different
    // value for the PROJECT_ARCHIVE_FILE_PATH key, it is necessary to
    // create a new instance of Props to make sure these different values
    // are isolated from each other.
    final Props prop = new Props(this.props);
    prop.putAll(additionalProps);

    File file = null;
    final FlowLoader loader;

    try {
      file = unzipProject(archive, fileType);

      reports = validateProject(project, archive, file, prop);

      loader = this.flowLoaderFactory.createFlowLoader(file);
      reports.put(DIRECTORY_FLOW_REPORT_KEY, loader.loadProjectFlow(project, file));

    } finally {
      cleanUpProjectTempDir(file);
    }

    // Check the validation report.
    if (!isReportStatusValid(reports, project)) {
      return reports;
    }

    // Upload the project to DB and storage.
    persistProject(project, loader, archive, uploader, fileType);

    // Clean up project old installations after new project is uploaded successfully.
    cleanUpProjectOldInstallations(project);

    return reports;
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

  private Map<String, ValidationReport> validateProject(final Project project,
      final File archive, final File file, final Props prop) {
    prop.put(ValidatorConfigs.PROJECT_ARCHIVE_FILE_PATH,
        archive.getAbsolutePath());
    // Basically, we want to make sure that for different invocations to the
    // uploadProject method,
    // the validators are using different values for the
    // PROJECT_ARCHIVE_FILE_PATH configuration key.
    // In addition, we want to reload the validator objects for each upload, so
    // that we can change the validator configuration files without having to
    // restart Azkaban web server. If the XmlValidatorManager is an instance
    // variable, 2 consecutive invocations to the uploadProject
    // method might cause the second one to overwrite the
    // PROJECT_ARCHIVE_FILE_PATH configuration parameter
    // of the first, thus causing a wrong archive file path to be passed to the
    // validators. Creating a separate XmlValidatorManager object for each
    // upload will prevent this issue without having to add
    // synchronization between uploads. Since we're already reloading the XML
    // config file and creating validator objects for each upload, this does
    // not add too much additional overhead.
    final ValidatorManager validatorManager = new XmlValidatorManager(prop);
    log.info("Validating project " + archive.getName()
        + " using the registered validators "
        + validatorManager.getValidatorsInfo().toString());
    return validatorManager.validate(project, file);
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
      final User uploader, final String fileType) throws ProjectManagerException {
    synchronized (project) {
      if (loader instanceof DirectoryFlowLoader) {
        final DirectoryFlowLoader directoryFlowLoader = (DirectoryFlowLoader) loader;
        final int newVersion = this.projectLoader.getLatestProjectVersion(project) + 1;
        final Map<String, Flow> flows = directoryFlowLoader.getFlowMap();
        for (final Flow flow : flows.values()) {
          flow.setProjectId(project.getId());
          flow.setVersion(newVersion);
        }

        this.storageManager.uploadProject(project, newVersion, archive, uploader, fileType);

        log.info("Uploading flow to db " + archive.getName());
        this.projectLoader.uploadFlows(project, newVersion, flows.values());
        log.info("Changing project versions " + archive.getName());
        this.projectLoader.changeProjectVersion(project, newVersion,
            uploader.getUserId());
        project.setFlows(flows);
        log.info("Uploading Job properties");
        this.projectLoader.uploadProjectProperties(project, new ArrayList<>(
            directoryFlowLoader.getJobPropsMap().values()));
        log.info("Uploading Props properties");
        this.projectLoader.uploadProjectProperties(project, directoryFlowLoader.getPropsList());
        this.projectLoader.postEvent(project, EventType.UPLOADED, uploader.getUserId(),
            "Uploaded project files zip " + archive.getName());
      } else if (loader instanceof DirectoryYamlFlowLoader) {
        // Todo jamiesjc: upload yaml file to DB as a blob
      } else {
        throw new ProjectManagerException("Invalid type of flow loader.");
      }
    }
  }

  private void cleanUpProjectOldInstallations(final Project project)
      throws ProjectManagerException{
    log.info("Cleaning up old install files older than "
        + (project.getVersion() - this.projectVersionRetention));
    this.projectLoader.cleanOlderProjectVersion(project.getId(),
        project.getVersion() - this.projectVersionRetention);

    // Clean up storage
    this.storageManager.cleanupProjectArtifacts(project.getId());
  }

  private void cleanUpProjectTempDir(final File file) {
    log.info("Cleaning up temp files.");
    try {
      if (file != null) {
        FileUtils.deleteDirectory(file);
      }
    } catch (final IOException e) {
      log.error("Failed to delete temp directory", e);
      file.deleteOnExit();
    }
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
    return this.storageManager.getProjectFile(project.getId(), version);
  }

}
