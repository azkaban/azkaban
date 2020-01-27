package cloudflow.models;

import java.util.List;
import java.util.Objects;

public class Project {

    private String id;
    private String name;
    private String description;
    private String spaceId;
    private List<String> admins;
    private String createdOn;
    private String createdByUser;
    private String lastModifiedOn;
    private String lastModifiedByUser;
    private String latestVersion;

  public Project(String name, String description, String spaceId,
      List<String> admins) {
    this.name = name;
    this.description = description;
    this.spaceId = spaceId;
    this.admins = admins;
  }

  public Project() {

  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getSpaceId() {
    return spaceId;
  }

  public void setSpaceId(String spaceId) {
    this.spaceId = spaceId;
  }

  public List<String> getAdmins() {
    return admins;
  }

  public void setAdmins(List<String> admins) {
    this.admins = admins;
  }

  public String getCreatedOn() {
    return createdOn;
  }

  public void setCreatedOn(String createdOn) {
    this.createdOn = createdOn;
  }

  public String getCreatedByUser() {
    return createdByUser;
  }

  public void setCreatedByUser(String createdByUser) {
    this.createdByUser = createdByUser;
  }

  public String getLastModifiedOn() {
    return lastModifiedOn;
  }

  public void setLastModifiedOn(String lastModifiedOn) {
    this.lastModifiedOn = lastModifiedOn;
  }

  public String getLastModifiedByUser() {
    return lastModifiedByUser;
  }

  public void setLastModifiedByUser(String lastModifiedByUser) {
    this.lastModifiedByUser = lastModifiedByUser;
  }

  public String getLatestVersion() {
    return latestVersion;
  }

  public void setLatestVersion(String latestVersion) {
    this.latestVersion = latestVersion;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Project)) {
      return false;
    }

    Project project = (Project) o;
    return getId().equals(project.getId()) &&
        getName().equals(project.getName()) &&
        getDescription().equals(project.getDescription()) &&
        getSpaceId().equals(project.getSpaceId()) &&
        getAdmins().equals(project.getAdmins()) &&
        getCreatedOn().equals(project.getCreatedOn()) &&
        getCreatedByUser().equals(project.getCreatedByUser()) &&
        getLastModifiedOn().equals(project.getLastModifiedOn()) &&
        getLastModifiedByUser().equals(project.getLastModifiedByUser()) &&
        getLatestVersion().equals(project.getLatestVersion());
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(getId(), getName(), getDescription(), getSpaceId(), getAdmins(), getCreatedOn(),
            getCreatedByUser(), getLastModifiedOn(), getLastModifiedByUser(), getLatestVersion());
  }

  @Override
  public String toString() {
    return "Project{" +
        "id='" + id + '\'' +
        ", name='" + name + '\'' +
        ", description='" + description + '\'' +
        ", spaceId='" + spaceId + '\'' +
        ", admins=" + admins +
        ", createdOn='" + createdOn + '\'' +
        ", createdByUser='" + createdByUser + '\'' +
        ", lastModifiedOn='" + lastModifiedOn + '\'' +
        ", lastModifiedByUser='" + lastModifiedByUser + '\'' +
        ", latestVersion='" + latestVersion + '\'' +
        '}';
  }
}
