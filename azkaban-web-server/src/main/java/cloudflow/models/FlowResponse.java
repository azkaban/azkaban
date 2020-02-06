package cloudflow.models;

import java.util.List;
import java.util.Objects;

public class FlowResponse {
  private String id;
  private String name;
  private int flowVersionCount;
  private String projectId;
  private String projectVersion;
  private List<String> admins;
  private String createdByUser;
  private long createdOn;
  private String ModifiedByUser;
  private long modifiedOn;
  private FlowVersion lastVersion;
  //@JsonInclude(Include.NON_NULL)
  private List<FlowVersion> flowVersions;

  public FlowResponse() {
  }

  public FlowResponse(String id, String name, int flowVersionCount, String projectId,
      String projectVersion, List<String> admins, String createdByUser, long createdOn,
      String modifiedByUser, long modifiedOn, FlowVersion lastVersion,
      List<FlowVersion> flowVersions) {
    this.id = id;
    this.name = name;
    this.flowVersionCount = flowVersionCount;
    this.projectId = projectId;
    this.projectVersion = projectVersion;
    this.admins = admins;
    this.createdByUser = createdByUser;
    this.createdOn = createdOn;
    ModifiedByUser = modifiedByUser;
    this.modifiedOn = modifiedOn;
    this.lastVersion = lastVersion;
    this.flowVersions = flowVersions;
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

  public int getFlowVersionCount() {
    return flowVersionCount;
  }

  public void setFlowVersionCount(int flowVersionCount) {
    this.flowVersionCount = flowVersionCount;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getProjectVersion() {
    return projectVersion;
  }

  public void setProjectVersion(String projectVersion) {
    this.projectVersion = projectVersion;
  }

  public List<String> getAdmins() {
    return admins;
  }

  public void setAdmins(List<String> admins) {
    this.admins = admins;
  }

  public String getCreatedByUser() {
    return createdByUser;
  }

  public void setCreatedByUser(String createdByUser) {
    this.createdByUser = createdByUser;
  }

  public long getCreatedOn() {
    return createdOn;
  }

  public void setCreatedOn(long createdOn) {
    this.createdOn = createdOn;
  }

  public String getModifiedByUser() {
    return ModifiedByUser;
  }

  public void setModifiedByUser(String modifiedByUser) {
    ModifiedByUser = modifiedByUser;
  }

  public long getModifiedOn() {
    return modifiedOn;
  }

  public void setModifiedOn(long modifiedOn) {
    this.modifiedOn = modifiedOn;
  }

  public FlowVersion getLastVersion() {
    return lastVersion;
  }

  public void setLastVersion(FlowVersion lastVersion) {
    this.lastVersion = lastVersion;
  }

  public List<FlowVersion> getFlowVersions() {
    return flowVersions;
  }

  public void setFlowVersions(List<FlowVersion> flowVersions) {
    this.flowVersions = flowVersions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FlowResponse)) {
      return false;
    }
    FlowResponse that = (FlowResponse) o;
    return getFlowVersionCount() == that.getFlowVersionCount() &&
        getCreatedOn() == that.getCreatedOn() &&
        getModifiedOn() == that.getModifiedOn() &&
        Objects.equals(getId(), that.getId()) &&
        Objects.equals(getName(), that.getName()) &&
        Objects.equals(getProjectId(), that.getProjectId()) &&
        Objects.equals(getProjectVersion(), that.getProjectVersion()) &&
        Objects.equals(getAdmins(), that.getAdmins()) &&
        Objects.equals(getCreatedByUser(), that.getCreatedByUser()) &&
        Objects.equals(getModifiedByUser(), that.getModifiedByUser()) &&
        Objects.equals(getLastVersion(), that.getLastVersion()) &&
        Objects.equals(getFlowVersions(), that.getFlowVersions());
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(getId(), getName(), getFlowVersionCount(), getProjectId(), getProjectVersion(),
            getAdmins(), getCreatedByUser(), getCreatedOn(), getModifiedByUser(), getModifiedOn(),
            getLastVersion(), getFlowVersions());
  }
}
