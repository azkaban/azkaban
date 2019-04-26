CREATE TABLE ramp_exceptional_job_Items (
    rampId VARCHAR(45) NOT NULL,
    flowId VARCHAR(128) NOT NULL,
    jobId VARCHAR(128) NOT NULL,
    treatment VARCHAR(1) NOT NULL,
    timestamp BIGINT NULL,
    PRIMARY KEY (rampId, flowId, jobId)
);

CREATE INDEX idx_ramp_exceptional_job_Items
    ON ramp_exceptional_job_Items (rampId, flowId, jobId);
