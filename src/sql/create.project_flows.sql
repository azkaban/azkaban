CREATE TABLE project_flows (
	project_id INT NOT NULL,
	version INT NOT NULL,
	flow_id VARCHAR(128),
	modified_time BIGINT NOT NULL,
	encoding_type TINYINT,
	json BLOB,
	PRIMARY KEY (project_id, version, flow_id)
);

CREATE INDEX flow_index ON project_flows(project_id, version);
