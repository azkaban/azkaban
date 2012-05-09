package azkaban.project;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import azkaban.utils.Props;

/**
 * ProjectManager that stores everything on local file system.
 * The following global parameters should be set -
 * file.project.loader.path - The project install path where projects will be loaded installed to.
 */
public class FileProjectLoader implements ProjectLoader {
	public static final String DIRECTORY_PARAM = "file.project.loader.path";
    private static final Logger logger = Logger.getLogger(FileProjectLoader.class);
	private File projectDirectory;
	
    public FileProjectLoader(Props props) {
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
    
    @Override
    public Map<String, Project> loadAllProjects() {
        // TODO Auto-generated method stub
        return new HashMap<String, Project>();
    }

    @Override
    public void addProject(Project project) {
        
    }

    @Override
    public boolean removeProject(Project project) {
        // TODO Auto-generated method stub
        return false;
    }

    
}