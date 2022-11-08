package azkaban.flow;

public class FlowResourceRecommendation implements Cloneable {
  private int id;
  private int projectId;
  private String flowId;
  private String cpuRecommendation;
  private String memoryRecommendation;
  private String diskRecommendation;

  public FlowResourceRecommendation(
      final int id,
      final int projectId,
      final String flowId
  ) {
    this(id, projectId, flowId, null, null, null);
  }

  public FlowResourceRecommendation(
      final int id,
      final int projectId,
      final String flowId,
      final String cpuRecommendation,
      final String memoryRecommendation,
      final String diskRecommendation) {
    this.id = id;
    this.projectId = projectId;
    this.flowId = flowId;
    this.cpuRecommendation = cpuRecommendation;
    this.memoryRecommendation = memoryRecommendation;
    this.diskRecommendation = diskRecommendation;
  }

  public int getId() {
    return this.id;
  }

  public int getProjectId() {
    return this.projectId;
  }

  public void setProjectId(int projectId) {
    this.projectId = projectId;
  }

  public String getFlowId() {
    return this.flowId;
  }

  public void setFlowId(final String flowId) {
    this.flowId = flowId;
  }

  public String getCpuRecommendation() {
    return this.cpuRecommendation;
  }

  public void setCpuRecommendation(final String cpuRecommendation) {
    this.cpuRecommendation = cpuRecommendation;
  }

  public String getMemoryRecommendation() {
    return this.memoryRecommendation;
  }

  public void setMemoryRecommendation(final String memoryRecommendation) {
    this.memoryRecommendation = memoryRecommendation;
  }

  public String getDiskRecommendation() {
    return this.diskRecommendation;
  }

  public void setDiskRecommendation(final String diskRecommendation) {
    this.diskRecommendation = diskRecommendation;
  }

  // Deep copy
  @Override
  public FlowResourceRecommendation clone() {
    try {
      return (FlowResourceRecommendation) super.clone();
    } catch (CloneNotSupportedException e) {
      // this shouldn't happen, since we are Cloneable
      throw new InternalError(e);
    }
  }
}
