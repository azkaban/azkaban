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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import azkaban.flow.Flow;
import azkaban.project.ProjectLogEvent.EventType;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.user.Permission.Type;
import azkaban.utils.DirectoryFlowLoader;
import azkaban.utils.Props;
import azkaban.utils.Utils;

public class ProjectManager {
	private static final Logger logger = Logger.getLogger(ProjectManager.class);
	
	private ConcurrentHashMap<Integer, Project> projectsById = new ConcurrentHashMap<Integer, Project>();
	private ConcurrentHashMap<String, Project> projectsByName = new ConcurrentHashMap<String, Project>();
	private final ProjectLoader projectLoader;
	private final Props props;
	private final File tempDir;
	private final int projectVersionRetention;
	private final boolean creatorDefaultPermissions;
	
	public ProjectManager(ProjectLoader loader, Props props) {
		this.projectLoader = loader;
		this.props = props;
		this.tempDir = new File(this.props.getString("project.temp.dir", "temp"));
		this.projectVersionRetention = (props.getInt("project.version.retention", 3));
		logger.info("Project version retention is set to " + projectVersionRetention);
		
		this.creatorDefaultPermissions = props.getBoolean("creator.default.proxy", true);
		
		if (!tempDir.exists()) {
			tempDir.mkdirs();
		}
		
		loadAllProjects();
	}

	private void loadAllProjects() {
		List<Project> projects;
		try {
			projects = projectLoader.fetchAllActiveProjects();
		} catch (ProjectManagerException e) {
			throw new RuntimeException("Could not load projects from store.", e);
		}
		for (Project proj: projects) {
			projectsByName.put(proj.getName(), proj);
			projectsById.put(proj.getId(), proj);
		}
		
		for (Project proj: projects) {
			loadAllProjectFlows(proj);
		}
	}
	
	private void loadAllProjectFlows(Project project) {
		try {
			List<Flow> flows = projectLoader.fetchAllProjectFlows(project);
			Map<String, Flow> flowMap = new HashMap<String, Flow>();
			for (Flow flow: flows) {
				flowMap.put(flow.getId(), flow);
			}
			
			project.setFlows(flowMap);
		}
		catch (ProjectManagerException e) {
			throw new RuntimeException("Could not load projects flows from store.", e);
		}
	}
	
	public List<String> getProjectNames() {
		return new ArrayList<String>(projectsByName.keySet());
	}

	public List<Project> getUserProjects(User user) {
		ArrayList<Project> array = new ArrayList<Project>();
		for (Project project : projectsById.values()) {
			Permission perm = project.getUserPermission(user);

			if (perm != null && (perm.isPermissionSet(Type.ADMIN) || perm.isPermissionSet(Type.READ))) {
				array.add(project);
			}
		}
		return array;
	}

  public List<Project> getGroupProjects(User user) {
    List<Project> array = new ArrayList<Project>();
    for (Project project : projectsById.values()) {
      if (project.hasGroupPermission(user, Type.READ)) {
        array.add(project);
      }
    }
    return array;
  }

	public List<Project> getUserProjectsByRegex(User user, String regexPattern) {
		List<Project> array = new ArrayList<Project>();
		Pattern pattern;
		try {
			pattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
		} catch (PatternSyntaxException e) {
			logger.error("Bad regex pattern " + regexPattern);
			return array;
		}
		
		
		for (Project project : projectsById.values()) {
			Permission perm = project.getUserPermission(user);

			if (perm != null && (perm.isPermissionSet(Type.ADMIN) || perm.isPermissionSet(Type.READ))) {
				if(pattern.matcher(project.getName()).find() ) {
					array.add(project);
				}
			}
		}
		return array;
	}

	public List<Project> getProjects() {
		return new ArrayList<Project>(projectsById.values());
	}

	public List<Project> getProjectsByRegex(String regexPattern) {
		List<Project> allProjects = new ArrayList<Project>();
		Pattern pattern;
		try {
			pattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
		} catch (PatternSyntaxException e) {
			logger.error("Bad regex pattern " + regexPattern);
			return allProjects;
		}
		for(Project project : getProjects()) {
			if(pattern.matcher(project.getName()).find()) {
				allProjects.add(project);
			}
		}
		return allProjects;
	}

	public Project getProject(String name) {
		return projectsByName.get(name);
	}

	public Project getProject(int id) {
		return projectsById.get(id);
	}

	public Project createProject(String projectName, String description, User creator) throws ProjectManagerException {
		if (projectName == null || projectName.trim().isEmpty()) {
			throw new ProjectManagerException("Project name cannot be empty.");
		} 
		else if (description == null || description.trim().isEmpty()) {
			throw new ProjectManagerException("Description cannot be empty.");
		} 
		else if (creator == null) {
			throw new ProjectManagerException("Valid creator user must be set.");
		} 
		else if (!projectName.matches("[a-zA-Z][a-zA-Z_0-9|-]*")) {
			throw new ProjectManagerException("Project names must start with a letter, followed by any number of letters, digits, '-' or '_'.");
		}

		if (projectsByName.contains(projectName)) {
			throw new ProjectManagerException("Project already exists.");
		}
		
		logger.info("Trying to create " + projectName + " by user " + creator.getUserId());
		Project newProject = projectLoader.createNewProject(projectName, description, creator);
		projectsByName.put(newProject.getName(), newProject);
		projectsById.put(newProject.getId(), newProject);
		
		if(creatorDefaultPermissions) {
		// Add permission to project
			projectLoader.updatePermission(newProject, creator.getUserId(), new Permission(Permission.Type.ADMIN), false);
			
			// Add proxy user 
			newProject.addProxyUser(creator.getUserId());
			try {
				updateProjectSetting(newProject);
			} catch (ProjectManagerException e) {
				e.printStackTrace();
				throw e;
			}
		}
		
		projectLoader.postEvent(newProject, EventType.CREATED, creator.getUserId(), null);
		
		return newProject;
	}

	public synchronized Project removeProject(Project project, User deleter) throws ProjectManagerException {
		projectLoader.removeProject(project, deleter.getUserId());
		projectLoader.postEvent(project, EventType.DELETED, deleter.getUserId(), null);
		
		projectsByName.remove(project.getName());
		projectsById.remove(project.getId());
		
		return project;
	}

	public void updateProjectDescription(Project project, String description, User modifier) throws ProjectManagerException {
		projectLoader.updateDescription(project, description, modifier.getUserId());
		projectLoader.postEvent(project, EventType.DESCRIPTION, modifier.getUserId(), "Description changed to " + description);
	}

	public List<ProjectLogEvent> getProjectEventLogs(Project project, int results, int skip) throws ProjectManagerException {
		return projectLoader.getProjectEvents(project, results, skip);
	}
	
	public Props getProperties(Project project, String source) throws ProjectManagerException {
		return projectLoader.fetchProjectProperty(project, source);
	}
	
	public Props getJobOverrideProperty(Project project, String jobName) throws ProjectManagerException {
		return projectLoader.fetchProjectProperty(project, jobName+".jor");
	}
	
	public void setJobOverrideProperty(Project project, Props prop, String jobName) throws ProjectManagerException {
		prop.setSource(jobName+".jor");
		Props oldProps = projectLoader.fetchProjectProperty(project, prop.getSource());
		if(oldProps == null) {
			projectLoader.uploadProjectProperty(project, prop);
		}
		else {
			projectLoader.updateProjectProperty(project, prop);
		}
		return;
	}

	public void updateProjectSetting(Project project) throws ProjectManagerException {
		projectLoader.updateProjectSettings(project);		
	}
	
	public void addProjectProxyUser(Project project, String proxyName, User modifier) throws ProjectManagerException {
		logger.info("User " + modifier.getUserId() + " adding proxy user " + proxyName + " to project " + project.getName());
		project.addProxyUser(proxyName);
		
		projectLoader.postEvent(project, EventType.PROXY_USER, modifier.getUserId(), "Proxy user " + proxyName + " is added to project.");
		updateProjectSetting(project);
	}
	
	public void removeProjectProxyUser(Project project, String proxyName, User modifier) throws ProjectManagerException {
		logger.info("User " + modifier.getUserId() + " removing proxy user " + proxyName + " from project " + project.getName());
		project.removeProxyUser(proxyName);
		
		projectLoader.postEvent(project, EventType.PROXY_USER, modifier.getUserId(), "Proxy user " + proxyName + " has been removed form the project.");
		updateProjectSetting(project);
	}
	
	public void updateProjectPermission(Project project, String name, Permission perm, boolean group, User modifier) throws ProjectManagerException {
		logger.info("User " + modifier.getUserId() + " updating permissions for project " + project.getName() + " for " + name + " " + perm.toString());
		projectLoader.updatePermission(project, name, perm, group);
		if (group) {
			projectLoader.postEvent(project, EventType.GROUP_PERMISSION, modifier.getUserId(), "Permission for group " + name + " set to " + perm.toString());
		}
		else {
			projectLoader.postEvent(project, EventType.USER_PERMISSION, modifier.getUserId(), "Permission for user " + name + " set to " + perm.toString());
		}
	}
	
	public void removeProjectPermission(Project project, String name, boolean group, User modifier) throws ProjectManagerException {
		logger.info("User " + modifier.getUserId() + " removing permissions for project " + project.getName() + " for " + name);
		projectLoader.removePermission(project, name, group);
		if (group) {
			projectLoader.postEvent(project, EventType.GROUP_PERMISSION, modifier.getUserId(), "Permission for group " + name + " removed.");
		}
		else {
			projectLoader.postEvent(project, EventType.USER_PERMISSION, modifier.getUserId(), "Permission for user " + name + " removed.");
		}
	}
	
	public void uploadProject(Project project, File archive, String fileType, User uploader) throws ProjectManagerException {
		logger.info("Uploading files to " + project.getName());
		
		// Unzip.
		File file = null;
		try {
			if (fileType == null) {
				throw new ProjectManagerException("Unknown file type for " + archive.getName());
			}
			else if ("zip".equals(fileType)) {
				file = unzipFile(archive);
			}
			else {
				throw new ProjectManagerException("Unsupported archive type for file " + archive.getName());
			}
		} catch(IOException e) {
			throw new ProjectManagerException("Error unzipping file.", e);
		}

		logger.info("Validating Flow for upload " + archive.getName());
		DirectoryFlowLoader loader = new DirectoryFlowLoader(logger);
		loader.loadProjectFlow(file);
		if(!loader.getErrors().isEmpty()) {
			logger.error("Error found in upload to " + project.getName() + ". Cleaning up.");
			
			try {
				FileUtils.deleteDirectory(file);
			} catch (IOException e) {
				file.deleteOnExit();
				e.printStackTrace();
			}
			
			StringBuffer errorMessage = new StringBuffer();
			errorMessage.append("Error found in upload. Cannot upload.\n");
			for (String error: loader.getErrors()) {
				errorMessage.append(error);
				errorMessage.append('\n');
			}

			throw new ProjectManagerException(errorMessage.toString());
		}
		
		Map<String, Props> jobProps = loader.getJobProps();
		List<Props> propProps = loader.getProps();
		
		synchronized(project) {
			int newVersion = projectLoader.getLatestProjectVersion(project) + 1;
			Map<String, Flow> flows = loader.getFlowMap();
			for (Flow flow: flows.values()) {
				flow.setProjectId(project.getId());
				flow.setVersion(newVersion);
			}
			
			logger.info("Uploading file to db " + archive.getName());
			projectLoader.uploadProjectFile(project, newVersion, fileType, archive.getName(), archive, uploader.getUserId());
			logger.info("Uploading flow to db " + archive.getName());
			projectLoader.uploadFlows(project, newVersion, flows.values());
			logger.info("Changing project versions " + archive.getName());
			projectLoader.changeProjectVersion(project, newVersion, uploader.getUserId());
			project.setFlows(flows);
			logger.info("Uploading Job properties");
			projectLoader.uploadProjectProperties(project, new ArrayList<Props>(jobProps.values()));
			logger.info("Uploading Props properties");
			projectLoader.uploadProjectProperties(project, propProps);
		}
	
		//TODO: find something else to load triggers
//		if(loadTriggerFromFile) {
//			logger.info("Loading triggers.");
//			Props triggerProps = new Props();
//			triggerProps.put("projectId", project.getId());
//			triggerProps.put("projectName", project.getName());
//			triggerProps.put("submitUser", uploader.getUserId());
//			try {
//				triggerManager.loadTriggerFromDir(file, triggerProps);
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//				logger.error("Failed to load triggers.", e);
//			}
//		}
		
		logger.info("Uploaded project files. Cleaning up temp files.");
		projectLoader.postEvent(project, EventType.UPLOADED, uploader.getUserId(), "Uploaded project files zip " + archive.getName());
		try {
			FileUtils.deleteDirectory(file);
		} catch (IOException e) {
			file.deleteOnExit();
			e.printStackTrace();
		}

		logger.info("Cleaning up old install files older than " + (project.getVersion() - projectVersionRetention));
		projectLoader.cleanOlderProjectVersion(project.getId(), project.getVersion() - projectVersionRetention);
	}
	
	public void updateFlow(Project project, Flow flow) throws ProjectManagerException {
		projectLoader.updateFlow(project, flow.getVersion(), flow);
	}
	
	private File unzipFile(File archiveFile) throws IOException {
		ZipFile zipfile = new ZipFile(archiveFile);
		File unzipped = Utils.createTempDir(tempDir);
		Utils.unzip(zipfile, unzipped);

		return unzipped;
	}

	public void postProjectEvent(Project project, EventType type, String user,String message) {
		projectLoader.postEvent(project, type, user, message);
	}


}
