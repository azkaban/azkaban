package cloudflow.services;

import azkaban.user.User;
import cloudflow.models.Project;
import java.util.List;

public interface ProjectService {

  String create(Project project, User user);
  Project get(String projectId);
  List<Project> getAll();
}
