package cloudflow.models;

import java.util.List;
import java.util.List;
import java.util.Objects;

public class Space {
  private int id;
  private String name;
  private String description;
  private List<String> admins;
  private List<String> watchers;
  private String createdOn;
  private String createdBy;
  private String modifiedOn;
  private String modifiedBy;

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Space space = (Space) o;
    return id == space.id &&
        Objects.equals(name, space.name) &&
        Objects.equals(description, space.description) &&
        Objects.equals(admins, space.admins) &&
        Objects.equals(watchers, space.watchers) &&
        Objects.equals(createdOn, space.createdOn) &&
        Objects.equals(createdBy, space.createdBy) &&
        Objects.equals(modifiedOn, space.modifiedOn) &&
        Objects.equals(modifiedBy, space.modifiedBy);
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(id, name, description, admins, watchers, createdOn, createdBy, modifiedOn,
            modifiedBy);
  }

  public Space() {
  }

  public Space(int id, String name, String description, List<String> admins,
      List<String> watchers, String createdOn, String createdBy, String modifiedOn,
      String modifiedBy) {
    this.id = id;
    this.name = name;
    this.description = description;
    this.admins = admins;
    this.watchers = watchers;
    this.createdOn = createdOn;
    this.createdBy = createdBy;
    this.modifiedOn = modifiedOn;
    this.modifiedBy = modifiedBy;
  }

  public String getCreatedOn() {
    return createdOn;
  }

  public void setCreatedOn(String createdOn) {
    this.createdOn = createdOn;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  public String getModifiedOn() {
    return modifiedOn;
  }

  public void setModifiedOn(String modifiedOn) {
    this.modifiedOn = modifiedOn;
  }

  public String getModifiedBy() {
    return modifiedBy;
  }

  public void setModifiedBy(String modifiedBy) {
    this.modifiedBy = modifiedBy;
  }



  public List<String> getAdmins() {
    return admins;
  }

  public void setAdmins(List<String> admins) {
    this.admins = admins;
  }

  public List<String> getWatchers() {
    return watchers;
  }

  public void setWatchers(List<String> watchers) {
    this.watchers = watchers;
  }

  public int getId() {
    return id;
  }

  @Override
  public String toString() {
    return "Space{" +
        "id=" + id +
        ", name='" + name + '\'' +
        ", description='" + description + '\'' +
        ", admins=" + admins +
        ", watchers=" + watchers +
        ", createdOn='" + createdOn + '\'' +
        ", createdBy='" + createdBy + '\'' +
        ", modifiedOn='" + modifiedOn + '\'' +
        ", modifiedBy='" + modifiedBy + '\'' +
        '}';
  }

  public void setId(int id) {
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

}

