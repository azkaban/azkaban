-- DB Migration from release 3.104.0 to 3.105.0
-- Expand the length of fields related to ramp feature
ALTER TABLE ramp_dependency CHANGE COLUMN dependency dependency VARCHAR(200) NOT NULL , CHANGE COLUMN defaultValue defaultValue VARCHAR(1000) NULL DEFAULT NULL;
ALTER TABLE ramp_items CHANGE COLUMN dependency dependency VARCHAR(200) NOT NULL , CHANGE COLUMN rampValue rampValue VARCHAR(1000) NOT NULL;
ALTER TABLE ramp_exceptional_flow_items CHANGE COLUMN flowId flowId VARCHAR(256) NOT NULL;
ALTER TABLE ramp_exceptional_job_items CHANGE COLUMN flowId flowId VARCHAR(256) NOT NULL;
