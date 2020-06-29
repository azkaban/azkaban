CREATE TABLE ramp (
    rampId VARCHAR(45) NOT NULL,
    rampPolicy VARCHAR(45) NOT NULL,
    maxFailureToPause INT NOT NULL DEFAULT 0,
    maxFailureToRampDown INT NOT NULL DEFAULT 0,
    isPercentageScaleForMaxFailure TINYINT NOT NULL DEFAULT 0,
    startTime BIGINT NOT NULL DEFAULT 0,
    endTime BIGINT NOT NULL DEFAULT 0,
    lastUpdatedTime BIGINT NOT NULL DEFAULT 0,
    numOfTrail INT NOT NULL DEFAULT 0,
    numOfFailure INT NOT NULL DEFAULT 0,
    numOfSuccess INT NOT NULL DEFAULT 0,
    numOfIgnored INT NOT NULL DEFAULT 0,
    isPaused TINYINT NOT NULL DEFAULT 0,
    rampStage TINYINT NOT NULL DEFAULT 0,
    isActive TINYINT NOT NULL DEFAULT 0,
    PRIMARY KEY (rampId)
);

CREATE INDEX idx_ramp
    ON ramp (rampId);
