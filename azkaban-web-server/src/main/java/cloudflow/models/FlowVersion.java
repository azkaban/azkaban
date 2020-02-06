package cloudflow.models;

import java.util.Objects;

public class FlowVersion {
  private String projectId;
  private String projectVersion;
  private String flowName;
  private String flowVersion;
  private long createTime;
  private String flowFileLocation;
  private String flowId;
  private Boolean experimental;
  private float dslVersion;
  private String createdBy;
  private Boolean locked;


  public FlowVersion(String projectId, String projectVersion, String flowName,
      String flowVersion, long createTime, String flowFileLocation,
      String flowId, Boolean experimental, float dslVersion, String createdBy,
      Boolean locked) {
    this.projectId = projectId;
    this.projectVersion = projectVersion;
    this.flowName = flowName;
    this.flowVersion = flowVersion;
    this.createTime = createTime;
    this.flowFileLocation = flowFileLocation;
    this.flowId = flowId;
    this.experimental = experimental;
    this.dslVersion = dslVersion;
    this.createdBy = createdBy;
    this.locked = locked;
  }

  public FlowVersion() {

  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getFlowId() {
    return flowId;
  }

  public void setFlowId(String flowId) {
    this.flowId = flowId;
  }

  public String getProjectVersion() {
    return projectVersion;
  }

  public void setProjectVersion(String projectVersion) {
    this.projectVersion = projectVersion;
  }

  public String getFlowName() {
    return flowName;
  }

  public void setFlowName(String flowName) {
    this.flowName = flowName;
  }

  public String getFlowVersion() {
    return flowVersion;
  }

  public void setFlowVersion(String flowVersion) {
    this.flowVersion = flowVersion;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public String getFlowFileLocation() {
    return flowFileLocation;
  }

  public void setFlowFileLocation(String flowFileLocation) {
    this.flowFileLocation = flowFileLocation;
  }

  public Boolean getExperimental() {
    return experimental;
  }

  public void setExperimental(Boolean experimental) {
    this.experimental = experimental;
  }

  public float getDslVersion() {
    return dslVersion;
  }

  public void setDslVersion(float dslVersion) {
    this.dslVersion = dslVersion;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  public Boolean getLocked() {
    return locked;
  }

  public void setLocked(Boolean locked) {
    this.locked = locked;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FlowVersion)) {
      return false;
    }
    FlowVersion that = (FlowVersion) o;
    return getCreateTime() == that.getCreateTime() &&
        Float.compare(that.getDslVersion(), getDslVersion()) == 0 &&
        Objects.equals(getProjectId(), that.getProjectId()) &&
        Objects.equals(getProjectVersion(), that.getProjectVersion()) &&
        Objects.equals(getFlowName(), that.getFlowName()) &&
        Objects.equals(getFlowVersion(), that.getFlowVersion()) &&
        Objects.equals(getFlowFileLocation(), that.getFlowFileLocation()) &&
        Objects.equals(getFlowId(), that.getFlowId()) &&
        Objects.equals(getExperimental(), that.getExperimental()) &&
        Objects.equals(getCreatedBy(), that.getCreatedBy()) &&
        Objects.equals(getLocked(), that.getLocked());
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(getProjectId(), getProjectVersion(), getFlowName(), getFlowVersion(), getCreateTime(),
            getFlowFileLocation(), getFlowId(), getExperimental(), getDslVersion(), getCreatedBy(),
            getLocked());
  }
}

