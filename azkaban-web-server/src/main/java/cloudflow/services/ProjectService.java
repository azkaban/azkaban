package cloudflow.services;

import azkaban.user.User;
import cloudflow.models.Project;
import java.util.List;

public interface ProjectService {

  String createProject(Project project, User user);
  Project getProject(String projectId);
  List<Project> getAllProjects(User user);
}
