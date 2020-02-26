package cloudflow.models;

import static java.util.Objects.requireNonNull;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions.FailureAction;


public class ExecutionBasicResponse {
  private final String executionId;
  private final String submitUser;
  private final long submitTime;
  private final String concurrentOption;
  private final FailureAction failureAction;
  private final boolean notifyOnFirstFailure;
  private final boolean notifyFailureOnExecutionComplete;
  private final String experimentId;
  private final String description;
  private final String previousFlowExecutionId;

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

  public String getFailureAction() {
    return failureAction.getName();
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
