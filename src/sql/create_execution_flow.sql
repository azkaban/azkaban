CREATE TABLE execution_flows (
	exec_id INT NOT NULL AUTO_INCREMENT,
	project_id INT NOT NULL,
	version INT NOT NULL,
	flow_id VARCHAR(128) NOT NULL,
	status TINYINT,
	submit_user VARCHAR(64),
	submit_time BIGINT,
	update_time BIGINT,
	start_time BIGINT,
	end_time BIGINT,
	enc_type TINYINT,
	flow_data LONGBLOB,
	PRIMARY KEY (exec_id),
	INDEX start_time (start_time),
	INDEX end_time (end_time),
	INDEX time_range (start_time, end_time),
	INDEX (project_id, flow_id)
) ENGINE=InnoDB;
