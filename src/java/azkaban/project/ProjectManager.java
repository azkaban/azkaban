package azkaban.project;

import java.io.File;
import java.security.AccessControlException;
import java.util.List;

import azkaban.user.User;

public interface ProjectManager {
    
    public List<String> getProjectNames();
    
    public List<Project> getProjects(User user);
    
    public Project getProject(String name, User user) throws AccessControlException;
    
    public void uploadProject(String projectName, File projectDir, User uploader, boolean force) throws ProjectManagerException;
    
    public Project createProject(String projectName, String description, User creator) throws ProjectManagerException;
    
    public Project removeProject(String projectName, User user) throws ProjectManagerException;
}