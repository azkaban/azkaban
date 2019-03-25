package azkaban.execapp.projectcache;

import java.util.Objects;

public class ProjectCacheKey {
  private final int projectId;
  private final int version;

  public ProjectCacheKey(int projectId, int version) {
    this.projectId = projectId;
    this.version = version;
  }

  public int getProjectId() {
    return projectId;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProjectCacheKey that = (ProjectCacheKey) o;
    return projectId == that.projectId &&
        version == that.version;
  }

  @Override
  public int hashCode() {
    return Objects.hash(projectId, version);
  }
}
