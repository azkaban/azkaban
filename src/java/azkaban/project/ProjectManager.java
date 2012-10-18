package azkaban.project;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;

import azkaban.user.User;
import azkaban.utils.Props;

public interface ProjectManager {

	public List<String> getProjectNames();

	public List<Project> getUserProjects(User user);
	
	public List<Project> getUserProjectsByRe(User user, String searchTerm);
	
	public List<Project> getProjects();
	
	public List<Project> getProjectsByRe(String searchTerm);

	public void commitProject(String name) throws ProjectManagerException;

	public Project getProject(String name);

	public void uploadProject(String projectName, File projectDir, User uploader, boolean force) throws ProjectManagerException;

	public Project createProject(String projectName, String description, User creator) throws ProjectManagerException;

	public Project removeProject(String projectName) throws ProjectManagerException;

	public Props getProperties(String projectName, String source) throws ProjectManagerException;

	public Props getProperties(Project project, String source) throws ProjectManagerException;

	public HashMap<String, Props> getAllFlowProperties(Project project, String flowId) throws ProjectManagerException;

	public void copyProjectSourceFilesToDirectory(Project project, File directory) throws ProjectManagerException;

	public void getProjectLogs(String projectId, long tailBytes, long skipBytes, Writer writer) throws IOException;

	


}