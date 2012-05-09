package azkaban.project;

import java.io.File;
import java.util.Map;

import azkaban.utils.Props;

public interface ProjectLoader {

    public Map<String, Project> loadAllProjects();

    public void addProject(Project project);

    public boolean removeProject(Project project);
}
