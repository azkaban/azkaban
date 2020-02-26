package cloudflow.models;

import azkaban.executor.Status;
import java.util.List;

public class JobExecution {

    private String executionId;
    private Long startTime;
    private Long endTime;
    private Status status;
    private List<NodeExecutionAttempt> attempts;


    public String getExecutionId() {
        return executionId;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public List<NodeExecutionAttempt> getAttempts() {
        return attempts;
    }

    public void setAttempts(List<NodeExecutionAttempt> attempts) {
        this.attempts = attempts;
    }
}
