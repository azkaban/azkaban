-- execution_logs and execution_jobs can grow very big
-- partition execution_flows, execution_jobs, and execution_logs by the range of exec_id
-- can improve the query performance and make retention easier


-- 1. partition execution_logs
CREATE TABLE execution_logs__new (
	exec_id INT NOT NULL,
	name VARCHAR(640) NOT NULL COMMENT 'embedded_flow1:embedded_flow_2:embedded_flow_3:job',
	attempt INT DEFAULT '0',
	enc_type TINYINT,
	start_byte INT NOT NULL DEFAULT '0',
	end_byte INT,
	log LONGBLOB,
	upload_time BIGINT,
	PRIMARY KEY (exec_id, name, attempt, start_byte)
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

INSERT IGNORE INTO execution_logs__new
SELECT * FROM execution_logs;


-- 2. partition execution_jobs
CREATE TABLE execution_jobs__new (
	exec_id INT NOT NULL,
	project_id INT NOT NULL,
	version INT NOT NULL,
	flow_id VARCHAR(256) NOT NULL,
	job_id VARCHAR(512) NOT NULL,
	attempt INT DEFAULT '0',
	start_time BIGINT,
	end_time BIGINT,
	status TINYINT,
	input_params LONGBLOB,
	output_params LONGBLOB,
	attachments LONGBLOB,
	PRIMARY KEY (exec_id, job_id, attempt)
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

INSERT IGNORE INTO execution_jobs__new
SELECT * FROM execution_jobs;


-- 3. partition execution_flows
CREATE TABLE execution_flows__new (
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

INSERT IGNORE INTO execution_flows__new
SELECT * FROM execution_flows;


-- 4. rename tables
RENAME TABLE execution_logs  TO execution_logs__old,
             execution_jobs  TO execution_jobs__old,
             execution_flows TO execution_flows__old;

RENAME TABLE execution_logs__new  TO execution_logs,
             execution_jobs__new  TO execution_jobs,
             execution_flows__new TO execution_flows;

CREATE INDEX ex_log_upload_time ON execution_logs(upload_time);

CREATE INDEX ex_job_id ON execution_jobs(project_id, job_id);

CREATE INDEX ex_flow_time_range ON execution_flows(start_time, end_time);
CREATE INDEX ex_flow_end_time ON execution_flows(end_time);
CREATE INDEX ex_flow_flows ON execution_flows(project_id, flow_id);
CREATE INDEX ex_flow_executor_id ON execution_flows(executor_id);

-- 5. analyze tables
ANALYZE TABLE execution_flows,
              execution_jobs,
              execution_logs;

-- 6. future partition maintenance (examples)
/*
ALTER TABLE execution_logs REORGANIZE
  PARTITION P999M INTO (
  PARTITION P034M VALUES LESS THAN (35000000),
  PARTITION P039M VALUES LESS THAN (40000000),
  PARTITION P999M VALUES LESS THAN (MAXVALUE)
);

ALTER TABLE execution_jobs REORGANIZE
  PARTITION P999M INTO (
  PARTITION P034M VALUES LESS THAN (35000000),
  PARTITION P039M VALUES LESS THAN (40000000),
  PARTITION P999M VALUES LESS THAN (MAXVALUE)
);

ALTER TABLE execution_flows REORGANIZE
  PARTITION P999M INTO (
  PARTITION P034M VALUES LESS THAN (35000000),
  PARTITION P039M VALUES LESS THAN (40000000),
  PARTITION P999M VALUES LESS THAN (MAXVALUE)
);
*/

