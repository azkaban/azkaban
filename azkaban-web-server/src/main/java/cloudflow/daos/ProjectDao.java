package cloudflow.daos;

import azkaban.user.User;
import cloudflow.models.Project;
import java.util.List;
import java.util.Optional;

public interface ProjectDao {
  String create(Project project, User user);
  Optional<Project> get(String projectId);
  List<Project> getAll();
}
