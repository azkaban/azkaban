package cloudflow.models;

import azkaban.executor.Status;

public class JobExecutionAttempt {
    private Integer id;
    private Long startTime;
    private Long endTime;
    private Status status;

    public JobExecutionAttempt(Integer id, Long startTime, Long endTime, Status status) {
        this.id = id;
        this.startTime = startTime;
        this.endTime = endTime;
        this.status = status;
    }

    public Integer getId() {
        return id;
    }

    public Long getStartTime() {
        return startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public Status getStatus() {
        return status;
    }
}
