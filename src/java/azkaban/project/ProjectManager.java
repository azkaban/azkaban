package azkaban.project;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import azkaban.user.User;
import azkaban.utils.Props;

public class ProjectManager {
    private static final Logger logger = Logger.getLogger(ProjectManager.class);
    private ConcurrentHashMap<String, Project> projects = new ConcurrentHashMap<String, Project>();
    private ProjectLoader loader;
    
    public ProjectManager(Props props, ProjectLoader loader) throws Exception {
    	projects.putAll(loader.loadAllProjects());
    }

    public List<String> getProjectNames() {
        return new ArrayList<String>(projects.keySet());
    }
    
    public Project getProject(String name) {
    	return projects.get(name);
    }
    
    public Project createProjects(String projectName, User creator) {
    	return null;
    }
    
    public Project loadProjects() {
    	return null;
    }
}