package azkaban.project;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;

/**
 * A project loader that stores everything on local file system.
 * The following global parameters should be set -
 * file.project.loader.path - The project install path where projects will be loaded installed to.
 */
public class FileProjectManager implements ProjectManager {
	public static final String DIRECTORY_PARAM = "file.project.loader.path";
	private static final String PROPERTIES_FILENAME = "project.json";
    private static final Logger logger = Logger.getLogger(FileProjectManager.class);
    private ConcurrentHashMap<String, Project> projects = new ConcurrentHashMap<String, Project>();

	private File projectDirectory;
	
    public FileProjectManager(Props props) {
    	setupDirectories(props);
    	loadAllProjects();
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
    
    private void loadAllProjects() {
    	File[] directories = projectDirectory.listFiles();
    	
    	for (File dir: directories) {
    		if (!dir.isDirectory()) {
    			logger.error("ERROR loading project from " + dir.getPath() + ". Not a directory." );
    		}
    		else {
    			File propertiesFile = new File(dir, PROPERTIES_FILENAME);
    			if (!propertiesFile.exists()) {
        			logger.error("ERROR loading project from " + dir.getPath() + ". Project file " + PROPERTIES_FILENAME + " not found." );
    			}
    			else {
    				Object obj = null;
    				try {
    					obj = JSONUtils.parseJSONFromFile(propertiesFile);
					} catch (IOException e) {
						logger.error("ERROR loading project from " + dir.getPath() + ". Project file " + PROPERTIES_FILENAME + " couldn't be read.", e );
						continue;
					}
    				
    				Project project = Project.projectFromObject(obj);
    				logger.info("Loading project " + project.getName());
    				projects.put(project.getName(), project);
    			}
    		}
    	}
    }
    
    public List<String> getProjectNames() {
        return new ArrayList<String>(projects.keySet());
    }
    
    public List<Project> getProjects(User user) {
    	ArrayList<Project> array = new ArrayList<Project>();
    	for(Project project : projects.values()) {
    		Permission perm = project.getUserPermission(user);
    		if (perm.isPermissionSet(Type.ADMIN) || perm.isPermissionSet(Type.READ)) {
    			array.add(project);
    		}
    	}
    	return array;
    }
    
    public Project getProject(String name, User user) throws AccessControlException {
    	Project project = projects.get(name);
    	if (project != null) {
    		Permission perm = project.getUserPermission(user);
    		if (perm.isPermissionSet(Type.ADMIN) || perm.isPermissionSet(Type.READ)) {
    			return project;
    		}
    		else {
    			throw new AccessControlException("Permission denied. Do not have read access.");
    		}
    	}
    	return null;
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
    	
    	File projectPath = new File(projectDirectory, projectName);
    	if (projectPath.exists()) {
    		throw new ProjectManagerException("Project already exists.");
    	}
    	
    	if(!projectPath.mkdirs()) {
    		throw new ProjectManagerException(
    				"Project directory " + projectName + 
    				" cannot be created in " + projectDirectory);
    	}
    	
    	Permission perm = new Permission(Type.ADMIN);
    	long time = System.currentTimeMillis();
    	
    	Project project = new Project(projectName);
    	project.setUserPermission(creator.getUserId(), perm);
    	project.setDescription(description);
    	project.setCreateTimestamp(time);
    	project.setLastModifiedTimestamp(time);
    	project.setLastModifiedUser(creator.getUserId());
    	
    	logger.info("Trying to create " + project.getName() + " by user " + creator.getUserId());
    	try {
			writeProjectFile(projectPath, project);
		} catch (IOException e) {
    		throw new ProjectManagerException(
    				"Project directory " + projectName + 
    				" cannot be created in " + projectDirectory, e);
		}
    	projects.put(projectName, project);
    	return project;
    }
    
    private void writeProjectFile(File directory, Project project) throws IOException {
    	Object object = project.toObject();
    	File outputFile = new File(directory, PROPERTIES_FILENAME);
    	logger.info("Writing project file " + outputFile);
    	String output = JSONUtils.toJSON(object, true);
    	
    	FileWriter writer = new FileWriter(outputFile);
    	try {
    		writer.write(output);
    	} catch (IOException e) {
    		if (writer != null) {
    			writer.close();
    		}
    		
    		throw e;
    	}
    	writer.close();
    }

	@Override
	public synchronized Project removeProjects(String projectName, User user) {
		return null;
	}
}