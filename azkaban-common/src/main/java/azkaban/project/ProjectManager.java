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

import azkaban.Constants;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.project.ProjectLogEvent.EventType;
import azkaban.project.validator.ValidationReport;
import azkaban.storage.ProjectStorageManager;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.CaseInsensitiveConcurrentHashMap;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class ProjectManager {

  private static final Logger logger = LoggerFactory.getLogger(ProjectManager.class);
  private final AzkabanProjectLoader azkabanProjectLoader;
  private final ProjectLoader projectLoader;
  private final Props props;
  private final boolean creatorDefaultPermissions;
  // Both projectsById and projectsByName cache need to be thread safe since they are accessed
  // from multiple threads concurrently without external synchronization for performance.
  private final ConcurrentHashMap<Integer, Project> projectsById =
      new ConcurrentHashMap<>();
  private final CaseInsensitiveConcurrentHashMap<Project> projectsByName =
      new CaseInsensitiveConcurrentHashMap<>();


  @Inject
  public ProjectManager(final AzkabanProjectLoader azkabanProjectLoader,
      final ProjectLoader loader,
      final ProjectStorageManager projectStorageManager,
      final Props props) {
    this.projectLoader = requireNonNull(loader);
    this.props = requireNonNull(props);
    this.azkabanProjectLoader = requireNonNull(azkabanProjectLoader);

    this.creatorDefaultPermissions =
        props.getBoolean("creator.default.proxy", true);

    loadAllProjects();
    logger.info("Loading whitelisted projects.");
    loadProjectWhiteList();
    logger.info("ProjectManager instance created.");
  }

  public boolean hasFlowTrigger(final Project project, final Flow flow)
      throws IOException, ProjectManagerException {
    final String flowFileName = flow.getId() + ".flow";
    final int latestFlowVersion = this.projectLoader.getLatestFlowVersion(project.getId(), flow
        .getVersion(), flowFileName);
    if (latestFlowVersion > 0) {
      final File tempDir = com.google.common.io.Files.createTempDir();
      final File flowFile;
      try {
        flowFile = this.projectLoader
            .getUploadedFlowFile(project.getId(), project.getVersion(),
                flowFileName, latestFlowVersion, tempDir);

        final FlowTrigger flowTrigger = FlowLoaderUtils.getFlowTriggerFromYamlFile(flowFile);
        return flowTrigger != null;
      } catch (final Exception ex) {
        logger.error("error in getting flow file", ex);
        throw ex;
      } finally {
        FlowLoaderUtils.cleanUpDir(tempDir);
      }
    } else {
      return false;
    }
  }

  private void loadAllProjects() {
    final List<Project> projects;
    logger.info("Loading active projects.");
    try {
      projects = this.projectLoader.fetchAllActiveProjects();
    } catch (final ProjectManagerException e) {
      throw new RuntimeException("Could not load projects from store.", e);
    }
    for (final Project proj : projects) {
      this.projectsByName.put(proj.getName(), proj);
      this.projectsById.put(proj.getId(), proj);
    }

    logger.info("Loading flows from active projects.");
    loadAllFlowsForAllProjects(projects);
  }

  private void loadAllFlowsForAllProjects(final List<Project> projects) {
    try {
      Map<Project, List<Flow>> projectToFlows = this.projectLoader.fetchAllFlowsForProjects(projects);

      // Load the flows into the project objects
      for (Map.Entry<Project, List<Flow>> entry : projectToFlows.entrySet()) {
        Project project = entry.getKey();
        List<Flow> flows = entry.getValue();

        final Map<String, Flow> flowMap = new HashMap<>();
        for (final Flow flow : flows) {
          flowMap.put(flow.getId(), flow);
        }

        project.setFlows(flowMap);
      }
    } catch (final ProjectManagerException e) {
      throw new RuntimeException("Could not load projects flows from store.", e);
    }
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
      logger.error("Bad regex pattern {}", regexPattern);
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
      logger.error("Bad regex pattern {}", regexPattern);
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
   * Checks if a project is active using project_id
   */
  public Boolean isActiveProject(final int id) {
    return this.projectsById.containsKey(id);
  }

  /**
   * fetch active project by project name. Queries the cache first then db if not found
   */
  public Project getProject(final String name) {
    Project fetchedProject = this.projectsByName.get(name);
    if (fetchedProject == null) {
      try {
        fetchedProject = this.projectLoader.fetchProjectByName(name);
        if (fetchedProject != null) {
          logger.info("Project {} not found in cache, fetched from DB.", name);
        } else {
          logger.info("No active project with name {} exists in cache or DB.", name);
        }
      } catch (final ProjectManagerException e) {
        logger.error("Could not load project from store.", e);
      }
    }
    return fetchedProject;
  }

  /**
   * fetch active project from cache and inactive projects from db by project_id
   */
  public Project getProject(final int id) {
    Project fetchedProject = this.projectsById.get(id);
    if (fetchedProject == null) {
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

    final Project newProject;
    synchronized (this) {
      if (this.projectsByName.containsKey(projectName)) {
        throw new ProjectManagerException("Project already exists.");
      }

      logger.info("Trying to create {} by user {}", projectName, creator.getUserId());
      newProject = this.projectLoader.createNewProject(projectName, description, creator);
      this.projectsByName.put(newProject.getName(), newProject);
      this.projectsById.put(newProject.getId(), newProject);
    }

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
   * Permanently delete all project files and properties data for all versions of a project and log
   * event in project_events table
   */
  public synchronized Project purgeProject(final Project project, final User deleter)
      throws ProjectManagerException {
    this.projectLoader.cleanOlderProjectVersion(project.getId(),
        project.getVersion() + 1, Collections.emptyList());
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

  public Props getPropertiesFromFlowFile(final Flow flow, final String jobName, final String
      flowFileName, final int flowVersion) throws ProjectManagerException {
    File tempDir = null;
    Props props = null;
    try {
      tempDir = Files.createTempDir();
      final File flowFile = this.projectLoader.getUploadedFlowFile(flow.getProjectId(), flow
          .getVersion(), flowFileName, flowVersion, tempDir);
      final String path =
          jobName == null ? flow.getId() : flow.getId() + Constants.PATH_DELIMITER + jobName;
      props = FlowLoaderUtils.getPropsFromYamlFile(path, flowFile);
    } catch (final Exception e) {
      this.logger.error("Failed to get props from flow file. " + e);
    } finally {
      FlowLoaderUtils.cleanUpDir(tempDir);
    }
    return props;
  }

  public Props getProperties(final Project project, final Flow flow, final String jobName,
      final String source) throws ProjectManagerException {
    if (FlowLoaderUtils.isAzkabanFlowVersion20(flow.getAzkabanFlowVersion())) {
      // Return the properties from the original uploaded flow file.
      return getPropertiesFromFlowFile(flow, jobName, source, 1);
    } else {
      return this.projectLoader.fetchProjectProperty(project, source);
    }
  }

  public Props getJobOverrideProperty(final Project project, final Flow flow, final String jobName,
      final String source) throws ProjectManagerException {
    if (FlowLoaderUtils.isAzkabanFlowVersion20(flow.getAzkabanFlowVersion())) {
      final int flowVersion = this.projectLoader
          .getLatestFlowVersion(flow.getProjectId(), flow.getVersion(), source);
      return getPropertiesFromFlowFile(flow, jobName, source, flowVersion);
    } else {
      return this.projectLoader
          .fetchProjectProperty(project, jobName + Constants.JOB_OVERRIDE_SUFFIX);
    }
  }

  public void setJobOverrideProperty(final Project project, final Flow flow, final Props prop,
      final String jobName, final String source, final User modifier)
      throws ProjectManagerException {
    File tempDir = null;
    Props oldProps = null;
    if (FlowLoaderUtils.isAzkabanFlowVersion20(flow.getAzkabanFlowVersion())) {
      try {
        tempDir = Files.createTempDir();
        final int flowVersion = this.projectLoader.getLatestFlowVersion(flow.getProjectId(), flow
            .getVersion(), source);
        final File flowFile = this.projectLoader.getUploadedFlowFile(flow.getProjectId(), flow
            .getVersion(), source, flowVersion, tempDir);
        final String path = flow.getId() + Constants.PATH_DELIMITER + jobName;
        oldProps = FlowLoaderUtils.getPropsFromYamlFile(path, flowFile);

        FlowLoaderUtils.setPropsInYamlFile(path, flowFile, prop);
        this.projectLoader
            .uploadFlowFile(flow.getProjectId(), flow.getVersion(), flowFile, flowVersion + 1);
      } catch (final Exception e) {
        this.logger.error("Failed to set job override property. " + e);
      } finally {
        FlowLoaderUtils.cleanUpDir(tempDir);
      }
    } else {
      prop.setSource(jobName + Constants.JOB_OVERRIDE_SUFFIX);
      oldProps = this.projectLoader.fetchProjectProperty(project, prop.getSource());

      if (oldProps == null) {
        this.projectLoader.uploadProjectProperty(project, prop);
      } else {
        this.projectLoader.updateProjectProperty(project, prop);
      }
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
    logger.info("User {} adding proxy user {} to project {}", modifier.getUserId(), proxyName,
        project.getName());
    project.addProxyUser(proxyName);

    this.projectLoader.postEvent(project, EventType.PROXY_USER,
        modifier.getUserId(), "Proxy user " + proxyName + " is added to project.");
    updateProjectSetting(project);
  }

  public void removeProjectProxyUser(final Project project, final String proxyName,
      final User modifier) throws ProjectManagerException {
    logger.info("User {} removing proxy user {} from project {}", modifier.getUserId(),
        proxyName, project.getName());
    project.removeProxyUser(proxyName);

    this.projectLoader.postEvent(project, EventType.PROXY_USER,
        modifier.getUserId(), "Proxy user " + proxyName
            + " has been removed form the project.");
    updateProjectSetting(project);
  }

  public void updateProjectPermission(final Project project, final String name,
      final Permission perm, final boolean group, final User modifier)
      throws ProjectManagerException {
    logger.info("User {} updating permissions for project {} for {} {}", modifier.getUserId(),
        project.getName(), name, perm.toString());
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
    logger.info("User {} removing permissions for project {} for {}", modifier.getUserId(),
        project.getName(), name);
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
   * This method retrieves the uploaded project zip file from DB. A temporary file is created to
   * hold the content of the uploaded zip file. This temporary file is provided in the
   * ProjectFileHandler instance and the caller of this method should call method
   * {@ProjectFileHandler.deleteLocalFile} to delete the temporary file.
   *
   * @param version - latest version is used if value is -1
   * @return ProjectFileHandler - null if can't find project zip file based on project name and
   * version
   */
  public ProjectFileHandler getProjectFileHandler(final Project project, final int version)
      throws ProjectManagerException {
    return this.azkabanProjectLoader.getProjectFile(project, version);
  }

  public Map<String, ValidationReport> uploadProject(final Project project,
      final File archive, final String fileType, final User uploader, final Props additionalProps,
      final String uploaderIPAddr)
      throws ProjectManagerException, ExecutorManagerException {
    return this.azkabanProjectLoader
        .uploadProject(project, archive, fileType, uploader, additionalProps, uploaderIPAddr);
  }

  public void updateFlow(final Project project, final Flow flow)
      throws ProjectManagerException {
    this.projectLoader.updateFlow(project, flow.getVersion(), flow);
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
