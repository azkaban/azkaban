/*
 * Copyright 2012 LinkedIn Corp.
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
 */

package azkaban.project;

import static java.util.Objects.requireNonNull;

import azkaban.flow.Flow;
import azkaban.project.ProjectLogEvent.EventType;
import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidationStatus;
import azkaban.project.validator.ValidatorConfigs;
import azkaban.project.validator.ValidatorManager;
import azkaban.project.validator.XmlValidatorManager;
import azkaban.storage.StorageManager;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import com.google.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class ProjectManager {

  private static final Logger logger = Logger.getLogger(ProjectManager.class);
  private final ProjectLoader projectLoader;
  private final StorageManager storageManager;
  private final Props props;
  private final File tempDir;
  private final int projectVersionRetention;
  private final boolean creatorDefaultPermissions;
  private final ConcurrentHashMap<Integer, Project> projectsById =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Project> projectsByName =
      new ConcurrentHashMap<>();

  @Inject
  public ProjectManager(final ProjectLoader loader, final StorageManager storageManager,
      final Props props) {
    this.projectLoader = requireNonNull(loader);
    this.storageManager = requireNonNull(storageManager);
    this.props = requireNonNull(props);

    this.tempDir = new File(this.props.getString("project.temp.dir", "temp"));
    this.projectVersionRetention =
        (props.getInt("project.version.retention", 3));
    logger.info("Project version retention is set to "
        + this.projectVersionRetention);

    this.creatorDefaultPermissions =
        props.getBoolean("creator.default.proxy", true);

    if (!this.tempDir.exists()) {
      this.tempDir.mkdirs();
    }

    // The prop passed to XmlValidatorManager is used to initialize all the
    // validators
    // Each validator will take certain key/value pairs from the prop to
    // initialize itself.
    final Props prop = new Props(props);
    prop.put(ValidatorConfigs.PROJECT_ARCHIVE_FILE_PATH, "initialize");
    // By instantiating an object of XmlValidatorManager, this will verify the
    // config files for the validators.
    new XmlValidatorManager(prop);
    loadAllProjects();
    loadProjectWhiteList();
  }

  private void loadAllProjects() {
    final List<Project> projects;
    try {
      projects = this.projectLoader.fetchAllActiveProjects();
    } catch (final ProjectManagerException e) {
      throw new RuntimeException("Could not load projects from store.", e);
    }
    for (final Project proj : projects) {
      this.projectsByName.put(proj.getName(), proj);
      this.projectsById.put(proj.getId(), proj);
    }

    for (final Project proj : projects) {
      loadAllProjectFlows(proj);
    }
  }

  private void loadAllProjectFlows(final Project project) {
    try {
      final List<Flow> flows = this.projectLoader.fetchAllProjectFlows(project);
      final Map<String, Flow> flowMap = new HashMap<>();
      for (final Flow flow : flows) {
        flowMap.put(flow.getId(), flow);
      }

      project.setFlows(flowMap);
    } catch (final ProjectManagerException e) {
      throw new RuntimeException("Could not load projects flows from store.", e);
    }
  }

  public List<String> getProjectNames() {
    return new ArrayList<>(this.projectsByName.keySet());
  }

  public Props getProps() {
    return this.props;
  }

  public List<Project> getUserProjects(final User user) {
    final ArrayList<Project> array = new ArrayList<>();
    for (final Project project : this.projectsById.values()) {
      final Permission perm = project.getUserPermission(user);

      if (perm != null
          && (perm.isPermissionSet(Type.ADMIN) || perm
          .isPermissionSet(Type.READ))) {
        array.add(project);
      }
    }
    return array;
  }

  public List<Project> getGroupProjects(final User user) {
    final List<Project> array = new ArrayList<>();
    for (final Project project : this.projectsById.values()) {
      if (project.hasGroupPermission(user, Type.READ)) {
        array.add(project);
      }
    }
    return array;
  }

  public List<Project> getUserProjectsByRegex(final User user, final String regexPattern) {
    final List<Project> array = new ArrayList<>();
    final Pattern pattern;
    try {
      pattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
    } catch (final PatternSyntaxException e) {
      logger.error("Bad regex pattern " + regexPattern);
      return array;
    }

    for (final Project project : this.projectsById.values()) {
      final Permission perm = project.getUserPermission(user);

      if (perm != null
          && (perm.isPermissionSet(Type.ADMIN) || perm
          .isPermissionSet(Type.READ))) {
        if (pattern.matcher(project.getName()).find()) {
          array.add(project);
        }
      }
    }
    return array;
  }

  public List<Project> getProjects() {
    return new ArrayList<>(this.projectsById.values());
  }

  public List<Project> getProjectsByRegex(final String regexPattern) {
    final List<Project> allProjects = new ArrayList<>();
    final Pattern pattern;
    try {
      pattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
    } catch (final PatternSyntaxException e) {
      logger.error("Bad regex pattern " + regexPattern);
      return allProjects;
    }
    for (final Project project : getProjects()) {
      if (pattern.matcher(project.getName()).find()) {
        allProjects.add(project);
      }
    }
    return allProjects;
  }

  /**
   * Checks if a project is active using project_name
   *
   * @param name
   */
  public Boolean isActiveProject(final String name) {
    return this.projectsByName.containsKey(name);
  }

  /**
   * Checks if a project is active using project_id
   *
   * @param name
   */
  public Boolean isActiveProject(final int id) {
    return this.projectsById.containsKey(id);
  }

  /**
   * fetch active project from cache and inactive projects from db by
   * project_name
   *
   * @param name
   * @return
   */
  public Project getProject(final String name) {
    Project fetchedProject = null;
    if (isActiveProject(name)) {
      fetchedProject = this.projectsByName.get(name);
    } else {
      try {
        fetchedProject = this.projectLoader.fetchProjectByName(name);
      } catch (final ProjectManagerException e) {
        logger.error("Could not load project from store.", e);
      }
    }
    return fetchedProject;
  }

  /**
   * fetch active project from cache and inactive projects from db by
   * project_id
   *
   * @param id
   * @return
   */
  public Project getProject(final int id) {
    Project fetchedProject = null;
    if (isActiveProject(id)) {
      fetchedProject = this.projectsById.get(id);
    } else {
      try {
        fetchedProject = this.projectLoader.fetchProjectById(id);
      } catch (final ProjectManagerException e) {
        logger.error("Could not load project from store.", e);
      }
    }
    return fetchedProject;
  }

  public Project createProject(final String projectName, final String description,
      final User creator) throws ProjectManagerException {
    if (projectName == null || projectName.trim().isEmpty()) {
      throw new ProjectManagerException("Project name cannot be empty.");
    } else if (description == null || description.trim().isEmpty()) {
      throw new ProjectManagerException("Description cannot be empty.");
    } else if (creator == null) {
      throw new ProjectManagerException("Valid creator user must be set.");
    } else if (!projectName.matches("[a-zA-Z][a-zA-Z_0-9|-]*")) {
      throw new ProjectManagerException(
          "Project names must start with a letter, followed by any number of letters, digits, '-' or '_'.");
    }

    if (this.projectsByName.containsKey(projectName)) {
      throw new ProjectManagerException("Project already exists.");
    }

    logger.info("Trying to create " + projectName + " by user "
        + creator.getUserId());
    final Project newProject =
        this.projectLoader.createNewProject(projectName, description, creator);
    this.projectsByName.put(newProject.getName(), newProject);
    this.projectsById.put(newProject.getId(), newProject);

    if (this.creatorDefaultPermissions) {
      // Add permission to project
      this.projectLoader.updatePermission(newProject, creator.getUserId(),
          new Permission(Permission.Type.ADMIN), false);

      // Add proxy user
      newProject.addProxyUser(creator.getUserId());
      try {
        updateProjectSetting(newProject);
      } catch (final ProjectManagerException e) {
        e.printStackTrace();
        throw e;
      }
    }

    this.projectLoader.postEvent(newProject, EventType.CREATED, creator.getUserId(),
        null);

    return newProject;
  }

  /**
   * Permanently delete all project files and properties data for all versions
   * of a project and log event in project_events table
   *
   * @param project
   * @param deleter
   * @return
   * @throws ProjectManagerException
   */
  public synchronized Project purgeProject(final Project project, final User deleter)
      throws ProjectManagerException {
    this.projectLoader.cleanOlderProjectVersion(project.getId(),
        project.getVersion() + 1);
    this.projectLoader
        .postEvent(project, EventType.PURGE, deleter.getUserId(), String
            .format("Purged versions before %d", project.getVersion() + 1));
    return project;
  }

  public synchronized Project removeProject(final Project project, final User deleter)
      throws ProjectManagerException {
    this.projectLoader.removeProject(project, deleter.getUserId());
    this.projectLoader.postEvent(project, EventType.DELETED, deleter.getUserId(),
        null);

    this.projectsByName.remove(project.getName());
    this.projectsById.remove(project.getId());

    return project;
  }

  public void updateProjectDescription(final Project project, final String description,
      final User modifier) throws ProjectManagerException {
    this.projectLoader.updateDescription(project, description, modifier.getUserId());
    this.projectLoader.postEvent(project, EventType.DESCRIPTION,
        modifier.getUserId(), "Description changed to " + description);
  }

  public List<ProjectLogEvent> getProjectEventLogs(final Project project,
      final int results, final int skip) throws ProjectManagerException {
    return this.projectLoader.getProjectEvents(project, results, skip);
  }

  public Props getProperties(final Project project, final String source)
      throws ProjectManagerException {
    return this.projectLoader.fetchProjectProperty(project, source);
  }

  public Props getJobOverrideProperty(final Project project, final String jobName)
      throws ProjectManagerException {
    return this.projectLoader.fetchProjectProperty(project, jobName + ".jor");
  }

  public void setJobOverrideProperty(final Project project, final Props prop, final String jobName,
      final User modifier)
      throws ProjectManagerException {
    prop.setSource(jobName + ".jor");
    final Props oldProps =
        this.projectLoader.fetchProjectProperty(project, prop.getSource());

    if (oldProps == null) {
      this.projectLoader.uploadProjectProperty(project, prop);
    } else {
      this.projectLoader.updateProjectProperty(project, prop);
    }

    final String diffMessage = PropsUtils.getPropertyDiff(oldProps, prop);

    this.projectLoader.postEvent(project, EventType.PROPERTY_OVERRIDE,
        modifier.getUserId(), diffMessage);
    return;
  }

  public void updateProjectSetting(final Project project)
      throws ProjectManagerException {
    this.projectLoader.updateProjectSettings(project);
  }

  public void addProjectProxyUser(final Project project, final String proxyName,
      final User modifier) throws ProjectManagerException {
    logger.info("User " + modifier.getUserId() + " adding proxy user "
        + proxyName + " to project " + project.getName());
    project.addProxyUser(proxyName);

    this.projectLoader.postEvent(project, EventType.PROXY_USER,
        modifier.getUserId(), "Proxy user " + proxyName
            + " is added to project.");
    updateProjectSetting(project);
  }

  public void removeProjectProxyUser(final Project project, final String proxyName,
      final User modifier) throws ProjectManagerException {
    logger.info("User " + modifier.getUserId() + " removing proxy user "
        + proxyName + " from project " + project.getName());
    project.removeProxyUser(proxyName);

    this.projectLoader.postEvent(project, EventType.PROXY_USER,
        modifier.getUserId(), "Proxy user " + proxyName
            + " has been removed form the project.");
    updateProjectSetting(project);
  }

  public void updateProjectPermission(final Project project, final String name,
      final Permission perm, final boolean group, final User modifier)
      throws ProjectManagerException {
    logger.info("User " + modifier.getUserId()
        + " updating permissions for project " + project.getName() + " for "
        + name + " " + perm.toString());
    this.projectLoader.updatePermission(project, name, perm, group);
    if (group) {
      this.projectLoader.postEvent(project, EventType.GROUP_PERMISSION,
          modifier.getUserId(), "Permission for group " + name + " set to "
              + perm.toString());
    } else {
      this.projectLoader.postEvent(project, EventType.USER_PERMISSION,
          modifier.getUserId(), "Permission for user " + name + " set to "
              + perm.toString());
    }
  }

  public void removeProjectPermission(final Project project, final String name,
      final boolean group, final User modifier) throws ProjectManagerException {
    logger.info("User " + modifier.getUserId()
        + " removing permissions for project " + project.getName() + " for "
        + name);
    this.projectLoader.removePermission(project, name, group);
    if (group) {
      this.projectLoader.postEvent(project, EventType.GROUP_PERMISSION,
          modifier.getUserId(), "Permission for group " + name + " removed.");
    } else {
      this.projectLoader.postEvent(project, EventType.USER_PERMISSION,
          modifier.getUserId(), "Permission for user " + name + " removed.");
    }
  }

  /**
   * This method retrieves the uploaded project zip file from DB. A temporary
   * file is created to hold the content of the uploaded zip file. This
   * temporary file is provided in the ProjectFileHandler instance and the
   * caller of this method should call method
   * {@ProjectFileHandler.deleteLocalFile}
   * to delete the temporary file.
   *
   * @param project
   * @param version - latest version is used if value is -1
   * @return ProjectFileHandler - null if can't find project zip file based on
   *         project name and version
   * @throws ProjectManagerException
   */
  public ProjectFileHandler getProjectFileHandler(final Project project, int version)
      throws ProjectManagerException {

    if (version == -1) {
      version = this.projectLoader.getLatestProjectVersion(project);
    }
    return this.storageManager.getProjectFile(project.getId(), version);
  }

  public Map<String, ValidationReport> uploadProject(final Project project,
      final File archive, final String fileType, final User uploader, final Props additionalProps)
      throws ProjectManagerException {
    logger.info("Uploading files to " + project.getName());

    // Unzip.
    File file = null;
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

    // Since props is an instance variable of ProjectManager, and each
    // invocation to the uploadProject manager needs to pass a different
    // value for the PROJECT_ARCHIVE_FILE_PATH key, it is necessary to
    // create a new instance of Props to make sure these different values
    // are isolated from each other.
    final Props prop = new Props(this.props);
    prop.putAll(additionalProps);
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
    logger.info("Validating project " + archive.getName()
        + " using the registered validators "
        + validatorManager.getValidatorsInfo().toString());
    final Map<String, ValidationReport> reports = validatorManager.validate(project, file);
    ValidationStatus status = ValidationStatus.PASS;
    for (final Entry<String, ValidationReport> report : reports.entrySet()) {
      if (report.getValue().getStatus().compareTo(status) > 0) {
        status = report.getValue().getStatus();
      }
    }
    if (status == ValidationStatus.ERROR) {
      logger.error("Error found in upload to " + project.getName()
          + ". Cleaning up.");

      try {
        FileUtils.deleteDirectory(file);
      } catch (final IOException e) {
        file.deleteOnExit();
        e.printStackTrace();
      }

      return reports;
    }

    final DirectoryFlowLoader loader =
        (DirectoryFlowLoader) validatorManager.getDefaultValidator();
    final Map<String, Props> jobProps = loader.getJobProps();
    final List<Props> propProps = loader.getProps();

    synchronized (project) {
      final int newVersion = this.projectLoader.getLatestProjectVersion(project) + 1;
      final Map<String, Flow> flows = loader.getFlowMap();
      for (final Flow flow : flows.values()) {
        flow.setProjectId(project.getId());
        flow.setVersion(newVersion);
      }

      this.storageManager.uploadProject(project, newVersion, archive, uploader);

      logger.info("Uploading flow to db " + archive.getName());
      this.projectLoader.uploadFlows(project, newVersion, flows.values());
      logger.info("Changing project versions " + archive.getName());
      this.projectLoader.changeProjectVersion(project, newVersion,
          uploader.getUserId());
      project.setFlows(flows);
      logger.info("Uploading Job properties");
      this.projectLoader.uploadProjectProperties(project, new ArrayList<>(
          jobProps.values()));
      logger.info("Uploading Props properties");
      this.projectLoader.uploadProjectProperties(project, propProps);
    }

    logger.info("Uploaded project files. Cleaning up temp files.");
    this.projectLoader.postEvent(project, EventType.UPLOADED, uploader.getUserId(),
        "Uploaded project files zip " + archive.getName());
    try {
      FileUtils.deleteDirectory(file);
    } catch (final IOException e) {
      file.deleteOnExit();
      e.printStackTrace();
    }

    logger.info("Cleaning up old install files older than "
        + (project.getVersion() - this.projectVersionRetention));
    this.projectLoader.cleanOlderProjectVersion(project.getId(),
        project.getVersion() - this.projectVersionRetention);

    return reports;
  }

  public void updateFlow(final Project project, final Flow flow)
      throws ProjectManagerException {
    this.projectLoader.updateFlow(project, flow.getVersion(), flow);
  }

  private File unzipFile(final File archiveFile) throws IOException {
    final ZipFile zipfile = new ZipFile(archiveFile);
    final File unzipped = Utils.createTempDir(this.tempDir);
    Utils.unzip(zipfile, unzipped);
    zipfile.close();

    return unzipped;
  }

  public void postProjectEvent(final Project project, final EventType type, final String user,
      final String message) {
    this.projectLoader.postEvent(project, type, user, message);
  }

  public boolean loadProjectWhiteList() {
    if (this.props.containsKey(ProjectWhitelist.XML_FILE_PARAM)) {
      ProjectWhitelist.load(this.props);
      return true;
    }
    return false;
  }
}
