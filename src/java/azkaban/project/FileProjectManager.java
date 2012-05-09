package azkaban.project;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import azkaban.user.User;
import azkaban.utils.Props;

/**
 * A project loader that stores everything on local file system.
 * The following global parameters should be set -
 * file.project.loader.path - The project install path where projects will be loaded installed to.
 */
public class FileProjectManager implements ProjectManager {
	public static final String DIRECTORY_PARAM = "file.project.loader.path";
    private static final Logger logger = Logger.getLogger(FileProjectManager.class);
    private ConcurrentHashMap<String, Project> projects = new ConcurrentHashMap<String, Project>();
	private File projectDirectory;
	
    public FileProjectManager(Props props) {
    	setupDirectories(props);
    }

    private void setupDirectories(Props props) {
    	String projectDir = props.getString(DIRECTORY_PARAM);
		logger.info("Using directory " + projectDir + " as the project directory.");
    	projectDirectory = new File(projectDir);
    	if (!projectDirectory.exists()) {
    		logger.info("Directory " + projectDir + " doesn't exist. Creating.");
    		if (projectDirectory.mkdirs()) {
    			logger.info("Directory creation was successful.");
    		}
    		else {
    			throw new RuntimeException("FileProjectLoader cannot create directory " + projectDirectory);
    		}
    	}
    	else if (projectDirectory.isFile()) {
			throw new RuntimeException("FileProjectManager directory " + projectDirectory + " is really a file.");
    	}
    }
    
    public List<String> getProjectNames() {
        return new ArrayList<String>(projects.keySet());
    }
    
    public Project getProject(String name) {
    	return projects.get(name);
    }
    
    @Override
    public synchronized Project createProjects(String projectName, String description, User creator) throws ProjectManagerException {
    	if (projectName == null || projectName.trim().isEmpty()) {
    		throw new ProjectManagerException("Project name cannot be empty.");
    	}
    	else if (description == null || description.trim().isEmpty()) {
    		throw new ProjectManagerException("Description cannot be empty.");
    	}
    	else if (creator == null) {
    		throw new ProjectManagerException("Valid creator user must be set.");
    	}

    	if (projects.contains(projectName)) {
    		throw new ProjectManagerException("Project already exists.");
    	}
    	
    	Project project = new Project(projectName);
    	return project;
    }
    
    public Project loadProjects() {
    	return null;
    }

	@Override
	public synchronized Project removeProjects(String projectName) {
		// TODO Auto-generated method stub
		return null;
	}
}