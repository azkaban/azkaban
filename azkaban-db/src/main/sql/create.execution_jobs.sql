CREATE TABLE execution_jobs (
	exec_id INT NOT NULL,
	project_id INT NOT NULL,
	version INT NOT NULL,
	flow_id VARCHAR(128) NOT NULL,
	job_id VARCHAR(128) NOT NULL,
	attempt INT,
	start_time BIGINT,
	end_time BIGINT,
	status TINYINT,
	input_params LONGBLOB,
	output_params LONGBLOB,
	attachments LONGBLOB,
	PRIMARY KEY (exec_id, job_id, attempt)
);

CREATE INDEX exec_job ON execution_jobs(exec_id, job_id);
CREATE INDEX exec_id ON execution_jobs(exec_id);
CREATE INDEX ex_job_id ON execution_jobs(project_id, job_id);
