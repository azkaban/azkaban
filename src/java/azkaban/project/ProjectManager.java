package azkaban.project;

import java.util.List;

import azkaban.user.User;

public interface ProjectManager {
    
    public List<String> getProjectNames();
    
    public Project getProject(String name);
    
    public Project createProjects(String projectName, String description, User creator) throws ProjectManagerException;
    
    public Project removeProjects(String projectName) throws ProjectManagerException;
}