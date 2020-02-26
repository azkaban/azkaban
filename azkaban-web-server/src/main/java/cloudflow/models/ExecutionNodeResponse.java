package cloudflow.models;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonPropertyOrder({"name", "type"})
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class ExecutionNodeResponse {

  @JsonProperty("name")
  private final String nodeId;  // maps to ExecutableNode.id

  @JsonProperty("type")
  private final ExecutionNodeType nodeType;

  private final long startTime;
  private final long endTime;
  private final String status;

  @JsonIgnore
  private final long updateTime;

  @Nullable
  private final FlowBasicResponse flowInfo;

  @JsonIgnore
  @Nullable
  private final String condition;

  @JsonIgnore
  @Nullable
  private final String nestedId;

  @JsonIgnore
  @Nullable
  private final List<String> inputNodeIds; // maps to ExecutableNode.inputNodes

  @JsonIgnore
  @Nullable
  private final String baseFlowId; // maps to ExecutableFlowBase.flowId

  @Nullable
  private final List<NodeExecutionAttempt> executionAttempts;

  @Nullable
  private final List<Optional<ExecutionNodeResponse>> nodeList;

  private ExecutionNodeResponse(String nodeId, ExecutionNodeType nodeType, long startTime,
      long endTime, String status, long updateTime, FlowBasicResponse flowInfo, String condition,
      String nestedId,
      List<String> inputNodeIds, String baseFlowId, List<NodeExecutionAttempt> executionAttempts,
      List<Optional<ExecutionNodeResponse>> nodeList) {
    this.nodeId = nodeId;
    this.nodeType = nodeType;
    this.startTime = startTime;
    this.endTime = endTime;
    this.status = status;
    this.updateTime = updateTime;
    this.flowInfo = flowInfo;
    this.condition = condition;
    this.nestedId = nestedId;
    this.inputNodeIds = inputNodeIds;
    this.baseFlowId = baseFlowId;
    this.executionAttempts = executionAttempts;
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

  public String getStatus() {
    return status;
  }

  public long getUpdateTime() {
    return updateTime;
  }

  public FlowBasicResponse getFlowInfo() {
    return flowInfo;
  }

  public ExecutionNodeType getNodeType() {
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

  public List<NodeExecutionAttempt> getExecutionAttempts() {
    return executionAttempts;
  }

  // todo(sshardool): see if there is a more direct way of dealing with optionals in jackson.
  public List<ExecutionNodeResponse> getNodeList() {
    if (nodeList != null) {
      return nodeList.stream().map(n -> n.orElse(null)).collect(Collectors.toList());
    }
    return null;
  }

  public static final class ExecutionNodeResponseBuilder {

    private String nodeId;  // maps to ExecutableNode.id
    private ExecutionNodeType nodeType;
    private long startTime;
    private long endTime;
    private String status;
    private long updateTime;
    private FlowBasicResponse flowInfo;
    private String condition = null;
    private String nestedId = null;
    private List<String> inputNodeIds = null; // maps to ExecutableNode.in
    private String baseFlowId = null; // maps to ExecutableFlowBase.flowId
    private List<NodeExecutionAttempt> executionAttempts;
    private List<Optional<ExecutionNodeResponse>> nodeList = null;

    private boolean isNodeIdSet = false;
    private boolean isNodeTypeSet = false;
    private boolean isStartTimeSet = false;
    private boolean isEndTimeSet = false;
    private boolean isStatusSet = false;
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

    public ExecutionNodeResponseBuilder withStatus(String status) {
      this.status = status;
      isStatusSet = true;
      return this;
    }

    public ExecutionNodeResponseBuilder withUpdateTime(long updateTime) {
      this.updateTime = updateTime;
      isUpdtateTimeSet = true;
      return this;
    }

    public ExecutionNodeResponseBuilder withNodeType(ExecutionNodeType nodeType) {
      isNodeTypeSet = true;
      this.nodeType = nodeType;
      return this;
    }

    public ExecutionNodeResponseBuilder withFlowInfo(FlowBasicResponse flowInfo) {
      this.flowInfo = flowInfo;
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

    public ExecutionNodeResponseBuilder withExecutionAttempt(
        List<NodeExecutionAttempt> executionAttempt) {
      this.executionAttempts = executionAttempt;
      return this;
    }

    public ExecutionNodeResponseBuilder withNodeList(
        List<Optional<ExecutionNodeResponse>> nodeList) {
      this.nodeList = nodeList;
      return this;
    }

    public ExecutionNodeResponse build() {
      checkState(isNodeIdSet, "node id is not set");
      checkState(isNodeTypeSet, "node type is not set");
      checkState(isStartTimeSet, "start time is not set");
      checkState(isEndTimeSet, "end time is not set");
      checkState(isStatusSet, "status is not set");
      checkState(isUpdtateTimeSet, "update time is not set");

      return new ExecutionNodeResponse(nodeId, nodeType, startTime, endTime, status, updateTime,
          flowInfo, condition, nestedId, inputNodeIds, baseFlowId, executionAttempts, nodeList);
    }
  }

  // Extract this to an independent class if it seems to overlap with the eventual
  // flow responce class (for the /flow endpoint)
  public static class FlowBasicResponse {

    private final String flowId;
    private final String flowName;
    private final String flowVersion;

    public FlowBasicResponse(String flowId, String flowName, String flowVersion) {
      this.flowId = flowId;
      this.flowName = flowName;
      this.flowVersion = flowVersion;
    }

    public String getFlowId() {
      return flowId;
    }

    public String getFlowName() {
      return flowName;
    }

    public String getFlowVersion() {
      return flowVersion;
    }
  }

  public enum ExecutionNodeType {
    ROOT_FLOW, EMBEDDED_FLOW, JOB
  }
}
