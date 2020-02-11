package cloudflow.models;

import static java.util.Objects.requireNonNull;

import azkaban.executor.ExecutableFlow;


public class ExecutionBasicResponse {
  private int executionId;
  private String submitUser;
  private long submitTime;
  private String experimentId;
  private String concurrentOption;
  private String failureAction;
  private boolean notifyOnFirstFailure;
  private boolean notifyFailureOnExecutionComplete;

  public ExecutionBasicResponse(ExecutableFlow executableFlow) {
    requireNonNull(executableFlow, "executable flow is null");
    this.executionId = executableFlow.getExecutionId();
    this.submitUser = executableFlow.getSubmitUser();
    this.submitTime = executableFlow.getSubmitTime();
    this.concurrentOption = executableFlow.getExecutionOptions().getConcurrentOption();
    this.failureAction = executableFlow.getExecutionOptions().getFailureAction().toString();
    this.notifyOnFirstFailure = executableFlow.getExecutionOptions().getNotifyOnFirstFailure();
    this.notifyFailureOnExecutionComplete = executableFlow.getExecutionOptions().getNotifyOnLastFailure();
    this.experimentId = "none"; //todo
  }

  public int getExecutionId() {
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
    return failureAction;
  }

  public boolean isNotifyOnFirstFailure() {
    return notifyOnFirstFailure;
  }

  public boolean isNotifyFailureOnExecutionComplete() {
    return notifyFailureOnExecutionComplete;
  }
}
