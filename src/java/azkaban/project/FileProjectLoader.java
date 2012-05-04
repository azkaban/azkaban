package azkaban.project;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import azkaban.utils.Props;

public class FileProjectLoader implements ProjectLoader {

    public FileProjectLoader(Props props) {}
    
    @Override
    public Map<String, Project> loadAllProjects() {
        // TODO Auto-generated method stub
        return new HashMap<String, Project>();
    }

    @Override
    public void addProject(Project project, File directory) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean removeProject(Project project) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void init(Props props) {
        // TODO Auto-generated method stub
        
    }
    
}