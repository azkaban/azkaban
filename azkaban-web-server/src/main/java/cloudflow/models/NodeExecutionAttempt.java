package cloudflow.models;

import azkaban.executor.Status;

public class NodeExecutionAttempt {
    private final int id;
    private final long startTime;
    private final long endTime;
    private Status status;

    public NodeExecutionAttempt(int id, long startTime, long endTime, Status status) {
        this.id = id;
        this.startTime = startTime;
        this.endTime = endTime;
        this.status = status;
    }

    public int getId() {
        return id;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public Status getStatus() {
        return status;
    }
}
