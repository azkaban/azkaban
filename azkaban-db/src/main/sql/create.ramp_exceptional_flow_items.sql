CREATE TABLE ramp_exceptional_flow_items (
    rampId VARCHAR(45) NOT NULL,
    flowId VARCHAR(128) NOT NULL,
    treatment VARCHAR(1) NOT NULL,
    timestamp BIGINT NULL,
    PRIMARY KEY (rampId, flowId)
);

CREATE INDEX idx_ramp_exceptional_flow_items
    ON ramp_exceptional_flow_items (rampId, flowId);
