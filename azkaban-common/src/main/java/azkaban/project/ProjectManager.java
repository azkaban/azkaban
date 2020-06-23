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
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
  private final ProjectCache cache;

  @Inject
  public ProjectManager(final AzkabanProjectLoader azkabanProjectLoader,
      final ProjectLoader loader,
      final ProjectStorageManager projectStorageManager,
      final Props props, final ProjectCache cache) {
    this.projectLoader = requireNonNull(loader);
    this.props = requireNonNull(props);
    this.azkabanProjectLoader = requireNonNull(azkabanProjectLoader);
    this.cache = requireNonNull(cache);
    this.creatorDefaultPermissions =
        props.getBoolean("creator.default.proxy", true);
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

  public Props getProps() {
    return this.props;
  }

  public List<Project> getUserProjects(final User user) {
    final ArrayList<Project> array = new ArrayList<>();
    for (final Project project : getProjects()) {
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
    for (final Project project : getProjects()) {
      if (project.hasGroupPermission(user, Type.READ)) {
        array.add(project);
      }
    }
    return array;
  }

  public List<Project> getUserProjectsByRegex(final User user, final String regexPattern) {
    final List<Project> array = new ArrayList<>();
    final List<Project> matches = getProjectsByRegex(regexPattern);
    for (final Project project : matches) {
      final Permission perm = project.getUserPermission(user);

      if (perm != null
          && (perm.isPermissionSet(Type.ADMIN) || perm
          .isPermissionSet(Type.READ))) {
        array.add(project);
      }
    }
    return array;
  }

  public List<Project> getProjects() {
    return new ArrayList<>(this.cache.getActiveProjects());
  }

  /**
   * This function matches the regex pattern with the names of all active projects, gets
   * corresponding ids and fetches the corresponding projects from the cache( cases : all projects
   * are present in cache / cache queries from DB and is updated).
   */
  public List<Project> getProjectsByRegex(final String regexPattern) {
    final Pattern pattern;
    try {
      pattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
    } catch (final PatternSyntaxException e) {
      logger.error("Bad regex pattern {}", regexPattern);
      return Collections.emptyList();
    }
    return this.cache.getProjectsWithSimilarNames(pattern);
  }


  /**
   * Checks if a project is active using project_id. getProject(id) can also fetch he inactive
   * projects from DB. Thus we need to make sure project retrieved is present in the mapping which
   * consists of all the active projects. This map has key as project name in all the project cache
   * implementations.
   */
  public Boolean isActiveProject(final int id) {
    Project project = getProject(id);
    if (project == null) {
      return false;
    }
    project = getProject(project.getName());
    return project != null ? true : false;
  }

  /**
   * Fetch active project by project name. Queries the cache first then DB.
   */
  public Project getProject(final String name) {
    final Project fetchedProject = this.cache.getProjectByName(name).orElse(null);
    return fetchedProject;
  }

  /**
   * Fetch active/inactive project by project id. If active project not present in cache, fetches
   * from DB. Fetches inactive project from DB.
   */
  public Project getProject(final int id) {
    Project fetchedProject = null;
    try {
      fetchedProject = this.cache.getProjectById(id).orElse(null);
    } catch (final ProjectManagerException e) {
      logger.info("Could not load from store project with id:", id);
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
      if (getProject(projectName) != null) {
        throw new ProjectManagerException("Project already exists.");
      }
      logger.info("Trying to create {} by user {}", projectName, creator.getUserId());
      newProject = this.projectLoader.createNewProject(projectName, description, creator);
      this.cache.putProject(newProject);
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

    this.cache.removeProject(project);

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
