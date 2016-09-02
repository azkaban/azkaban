CREATE TABLE execution_flows (
	exec_id SERIAL NOT NULL ,
	executor_id INT DEFAULT NULL,
	project_id INT NOT NULL,
	version INT NOT NULL,
	flow_id VARCHAR(128) NOT NULL,
	status SMALLINT,
	submit_user VARCHAR(64),
	submit_time BIGINT,
	update_time BIGINT,
	start_time BIGINT,
	end_time BIGINT,
	enc_type SMALLINT,
	flow_data BYTEA,
	PRIMARY KEY (exec_id)
);

CREATE INDEX ex_flows_start_time ON execution_flows(start_time);
CREATE INDEX ex_flows_end_time ON execution_flows(end_time);
CREATE INDEX ex_flows_time_range ON execution_flows(start_time, end_time);
CREATE INDEX ex_flows_flows ON execution_flows(project_id, flow_id);
CREATE INDEX ex_flows_executor_id ON execution_flows(executor_id);