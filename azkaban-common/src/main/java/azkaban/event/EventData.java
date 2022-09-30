package azkaban.event;

import azkaban.executor.ExecutableNode;
import azkaban.executor.Status;

/**
 * Carries an immutable snapshot of the status data, suitable for asynchronous message passing.
 */
public class EventData {

  private final ExecutableNode node;
  private final Status status;
  private final String nestedId;
  private final String jobId;
  private final String nodeType;
  private final String projectName;
  private final String flowName;
  private final int executionId;

  /**
   * Creates a new EventData instance.
   *
   * @param node node.
   */
  public EventData(final ExecutableNode node) {
    this.node = node;
    this.status = node.getStatus();
    this.nestedId = node.getNestedId();
    this.jobId = node.getId();
    this.nodeType = node.getType();

    this.projectName = (node.getParentFlow() == null) ? null: node.getParentFlow().getProjectName();
    this.flowName = (node.getParentFlow() == null) ? null: node.getParentFlow().getFlowId();
    this.executionId = (node.getParentFlow() == null) ? -1: node.getParentFlow().getExecutionId();
  }

  public ExecutableNode getNode() {
    return this.node;
  }

  public Status getStatus() {
    return this.status;
  }

  public String getNestedId() {
    return this.nestedId;
  }

  public String getJobId() {
    return this.jobId;
  }

  public String getNodeType() {
    return this.nodeType;
  }

  public String getProjectName() {
    return this.projectName;
  }

  public String getFlowName() {
    return this.flowName;
  }

  public int getExecutionId() {
    return this.executionId;
  }
}
