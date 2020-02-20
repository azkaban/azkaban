package cloudflow.services;

import java.util.Map;
import org.codehaus.jackson.annotate.JsonCreator;

public class ExecutionParameters {
  private String flowId;
  private Integer flowVersion;
  private String description;
  private String experimentId;
  private String submitUser;

  private FailureAction failureAction;
  private Boolean notifyOnFirstFailure;
  private Boolean notifyFailureOnExecutionComplete;
  private ConcurrentOption concurrentOption;
  private Map<String, Map<String, Object>> properties;

  // used when rerunning an execution or executing partial DAGs
  private String previousFlowExecutionId;


  public String getFlowId() {
    return flowId;
  }

  public void setFlowId(String flowId) {
    this.flowId = flowId;
  }

  public Integer getFlowVersion() {
    return flowVersion;
  }

  public void setFlowVersion(Integer flowVersion) {
    this.flowVersion = flowVersion;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getExperimentId() {
    return experimentId;
  }

  public void setExperimentId(String experimentId) {
    this.experimentId = experimentId;
  }

  public String getSubmitUser() {
    return submitUser;
  }

  public void setSubmitUser(String submitUser) {
    this.submitUser = submitUser;
  }

  public FailureAction getFailureAction() {
    return failureAction;
  }

  public void setFailureAction(FailureAction failureAction) {
    this.failureAction = failureAction;
  }

  public Boolean isNotifyOnFirstFailure() {
    return notifyOnFirstFailure;
  }

  public void setNotifyOnFirstFailure(Boolean notifyOnFirstFailure) {
    this.notifyOnFirstFailure = notifyOnFirstFailure;
  }

  public Boolean isNotifyFailureOnExecutionComplete() {
    return notifyFailureOnExecutionComplete;
  }

  public void setNotifyFailureOnExecutionComplete(Boolean notifyFailureOnExecutionComplete) {
    this.notifyFailureOnExecutionComplete = notifyFailureOnExecutionComplete;
  }

  public ConcurrentOption getConcurrentOption() {
    return concurrentOption;
  }

  public void setConcurrentOption(ConcurrentOption concurrentOption) {
    this.concurrentOption = concurrentOption;
  }

  public Map<String, Map<String, Object>> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Map<String, Object>> properties) {
    this.properties = properties;
  }

  public String getPreviousFlowExecutionId() {
    return previousFlowExecutionId;
  }

  public void setPreviousFlowExecutionId(String previousFlowExecutionId) {
    this.previousFlowExecutionId = previousFlowExecutionId;
  }

  public enum FailureAction {
    FINISH_CURRENTLY_RUNNING("finishCurrent"),
    CANCEL_ALL("cancelImmediately"),
    FINISH_ALL_POSSIBLE("finishPossible");

    private final String name;
    private FailureAction(String name) {
            this.name = name;
        }

    public String getName() {
            return this.name;
        }

    @JsonCreator
    public static FailureAction valueFromName(String name) {
      for(FailureAction action: FailureAction.values()) {
        if(action.name.equals(name)) {
          return action;
        }
      }
      throw new IllegalArgumentException("No FailureAction for name " + name);
    }
  }

  public enum ConcurrentOption {
    CONCURRENT_OPTION_SKIP("skip"),
    CONCURRENT_OPTION_IGNORE("ignore");
    // not supported in the POC CONCURRENT_OPTION_PIPELINE("pipeline")

    private final String name;
    private ConcurrentOption(String name) {
            this.name = name;
        }

    public String getName() {
            return this.name;
        }

    @JsonCreator
    public static ConcurrentOption valueFromName(String name) {
      for(ConcurrentOption option: ConcurrentOption.values()) {
        if(option.name.equals(name)) {
          return option;
        }
      }
      throw new IllegalArgumentException("No ConcurrentOption for name " + name);
    }
  }
}
