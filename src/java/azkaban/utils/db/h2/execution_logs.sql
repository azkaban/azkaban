CREATE TABLE execution_logs (
	exec_id INT NOT NULL,
	name VARCHAR(128),
	attempt INT,
	enc_type TINYINT,
	start_byte INT,
	end_byte INT,
	log LONGBLOB,
	upload_time BIGINT,
	PRIMARY KEY (exec_id, name, attempt, start_byte)
);

CREATE INDEX ex_log_attempt ON execution_logs(exec_id, name, attempt);
CREATE INDEX ex_log_index ON execution_logs(exec_id, name);