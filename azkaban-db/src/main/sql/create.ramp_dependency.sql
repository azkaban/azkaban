CREATE TABLE ramp_dependency (
    dependency VARCHAR(45) NOT NULL,
    defaultValue VARCHAR (500),
    jobtypes VARCHAR (1000),
    PRIMARY KEY (dependency)
);

CREATE INDEX idx_ramp_dependency
  ON ramp_dependency(dependency);
