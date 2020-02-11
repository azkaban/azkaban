package cloudflow.models;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import com.sun.istack.internal.Nullable;
import java.util.List;

public class ExecutionNodeResponse {

  private final String nodeId;  // maps to ExecutableNode.id
  private final long startTime;
  private final long endTime;
  private final long updateTime;
  @Nullable
  private final String nodeType;
  @Nullable
  private final String condition;
  @Nullable
  private final String nestedId;
  @Nullable
  private final List<String> inputNodeIds; // maps to ExecutableNode.inputNodes
  @Nullable
  private final String baseFlowId; // maps to ExecutableFlowBase.flowId
  @Nullable
  private final List<ExecutionNodeResponse> nodeList;

  private ExecutionNodeResponse(String nodeId, long startTime, long endTime,
      long updateTime, String nodeType, String condition, String nestedId,
      List<String> inputNodeIds, String baseFlowId,
      List<ExecutionNodeResponse> nodeList) {
    this.nodeId = nodeId;
    this.startTime = startTime;
    this.endTime = endTime;
    this.updateTime = updateTime;
    this.nodeType = nodeType;
    this.condition = condition;
    this.nestedId = nestedId;
    this.inputNodeIds = inputNodeIds;
    this.baseFlowId = baseFlowId;
    this.nodeList = nodeList;
  }

  public String getNodeId() {
    return nodeId;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public long getUpdateTime() {
    return updateTime;
  }

  public String getNodeType() {
    return nodeType;
  }

  public String getCondition() {
    return condition;
  }

  public String getNestedId() {
    return nestedId;
  }

  public List<String> getInputNodeIds() {
    return inputNodeIds;
  }

  public String getBaseFlowId() {
    return baseFlowId;
  }

  public List<ExecutionNodeResponse> getNodeList() {
    return nodeList;
  }

  public static final class ExecutionNodeResponseBuilder {

    private String nodeId;  // maps to ExecutableNode.id
    private long startTime;
    private long endTime;
    private long updateTime;
    private String nodeType = null;
    private String condition = null;
    private String nestedId = null;
    private List<String> inputNodeIds = null; // maps to ExecutableNode.in
    private String baseFlowId = null; // maps to ExecutableFlowBase.flowId
    private List<ExecutionNodeResponse> nodeList = null;

    private boolean isNodeIdSet = false;
    private boolean isStartTimeSet = false;
    private boolean isEndTimeSet = false;
    private boolean isUpdtateTimeSet = false;

    private ExecutionNodeResponseBuilder() {
    }

    public static ExecutionNodeResponseBuilder newBuilder() {
      return new ExecutionNodeResponseBuilder();
    }

    public ExecutionNodeResponseBuilder withNodeId(String nodeId) {
      requireNonNull(nodeId, "node id is null");
      this.nodeId = nodeId;
      isNodeIdSet = true;
      return this;
    }

    public ExecutionNodeResponseBuilder withStartTime(long startTime) {
      this.startTime = startTime;
      isStartTimeSet = true;
      return this;
    }

    public ExecutionNodeResponseBuilder withEndTime(long endTime) {
      this.endTime = endTime;
      isEndTimeSet = true;
      return this;
    }

    public ExecutionNodeResponseBuilder withUpdateTime(long updateTime) {
      this.updateTime = updateTime;
      isUpdtateTimeSet = true;
      return this;
    }

    public ExecutionNodeResponseBuilder withNodeType(String nodeType) {
      this.nodeType = nodeType;
      return this;
    }

    public ExecutionNodeResponseBuilder withCondition(String condition) {
      this.condition = condition;
      return this;
    }

    public ExecutionNodeResponseBuilder withNestedId(String nestedId) {
      this.nestedId = nestedId;
      return this;
    }

    public ExecutionNodeResponseBuilder withInputNodeIds(List<String> inputNodeIds) {
      this.inputNodeIds = inputNodeIds;
      return this;
    }

    public ExecutionNodeResponseBuilder withBaseFlowId(String baseFlowId) {
      this.baseFlowId = baseFlowId;
      return this;
    }

    public ExecutionNodeResponseBuilder withNodeList(List<ExecutionNodeResponse> nodeList) {
      this.nodeList = nodeList;
      return this;
    }

    public ExecutionNodeResponse build() {
      checkState(isNodeIdSet, "node id is not set");
      checkState(isStartTimeSet, "start time is not set");
      checkState(isEndTimeSet, "end time is not set");
      checkState(isUpdtateTimeSet, "update time is not set");

      return new ExecutionNodeResponse(nodeId, startTime, endTime, updateTime,
          nodeType, condition, nestedId, inputNodeIds, baseFlowId, nodeList);
    }
  }
}
