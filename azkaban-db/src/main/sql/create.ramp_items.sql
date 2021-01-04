CREATE TABLE ramp_items (
  rampId VARCHAR(45) NOT NULL,
  dependency VARCHAR(200) NOT NULL,
  rampValue VARCHAR (1000) NOT NULL,
  PRIMARY KEY (rampId, dependency)
);

CREATE INDEX idx_ramp_items
    ON ramp_items (rampId, dependency);
