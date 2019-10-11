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
	executor_id INT,
	use_executor INT,
	flow_priority TINYINT NOT NULL DEFAULT '5',
	PRIMARY KEY (exec_id)
)
PARTITION BY RANGE (exec_id) (
  PARTITION P004M VALUES LESS THAN (05000000),
  PARTITION P009M VALUES LESS THAN (10000000),
  PARTITION P014M VALUES LESS THAN (15000000),
  PARTITION P019M VALUES LESS THAN (20000000),
  PARTITION P024M VALUES LESS THAN (25000000),
  PARTITION P029M VALUES LESS THAN (30000000),
  PARTITION P999M VALUES LESS THAN (MAXVALUE)
);

CREATE INDEX ex_flow_time_range ON execution_flows(start_time, end_time);
CREATE INDEX ex_flow_end_time ON execution_flows(end_time);
CREATE INDEX ex_flow_flows ON execution_flows(project_id, flow_id);
CREATE INDEX ex_flow_executor_id ON execution_flows(executor_id);