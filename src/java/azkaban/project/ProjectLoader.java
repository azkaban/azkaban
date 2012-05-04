package azkaban.project;

import java.io.File;
import java.util.Map;

import azkaban.utils.Props;

public interface ProjectLoader {
    public void init(Props props);

    public Map<String, Project> loadAllProjects();

    public void addProject(Project project, File directory);

    public boolean removeProject(Project project);
}
