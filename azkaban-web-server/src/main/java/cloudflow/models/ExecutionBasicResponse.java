package cloudflow.models;

import static java.util.Objects.requireNonNull;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions.FailureAction;


public class ExecutionBasicResponse {
  private String executionId;
  private String submitUser;
  private long submitTime;
  private String concurrentOption;
  private FailureAction failureAction;
  private boolean notifyOnFirstFailure;
  private boolean notifyFailureOnExecutionComplete;

  private String experimentId;
  private String description;
  private String previousFlowExecutionId;

  public ExecutionBasicResponse(ExecutableFlow executableFlow) {
    requireNonNull(executableFlow, "executable flow is null");
    this.executionId = Integer.toString(executableFlow.getExecutionId());
    this.submitUser = executableFlow.getSubmitUser();
    this.submitTime = executableFlow.getSubmitTime();
    this.concurrentOption = executableFlow.getExecutionOptions().getConcurrentOption();
    this.failureAction = executableFlow.getExecutionOptions().getFailureAction();
    this.notifyOnFirstFailure = executableFlow.getExecutionOptions().getNotifyOnFirstFailure();
    this.notifyFailureOnExecutionComplete = executableFlow.getExecutionOptions().getNotifyOnLastFailure();

    //todo (sshardool): add these missing fields once available.
    this.experimentId = "defaultExperimentId";
    this.description = "defaultDescription";
    this.previousFlowExecutionId = "defaultPreviousExecutionId";
  }

  public String getExecutionId() {
    return executionId;
  }

  public String getSubmitUser() {
    return submitUser;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public String getExperimentId() {
    return experimentId;
  }

  public String getConcurrentOption() {
    return concurrentOption;
  }

  public FailureAction getFailureAction() {
    return failureAction;
  }

  public boolean isNotifyOnFirstFailure() {
    return notifyOnFirstFailure;
  }

  public boolean isNotifyFailureOnExecutionComplete() {
    return notifyFailureOnExecutionComplete;
  }

 public String getDescription() {
    return description;
  }

  public String getPreviousFlowExecutionId() {
    return previousFlowExecutionId;
  }
}
