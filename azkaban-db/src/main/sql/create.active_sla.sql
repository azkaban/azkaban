CREATE TABLE active_sla (
	exec_id INT NOT NULL,
	job_name VARCHAR(128) NOT NULL,
	check_time BIGINT NOT NULL,
	rule TINYINT NOT NULL,
	enc_type TINYINT,
	options LONGBLOB NOT NULL,
	primary key(exec_id, job_name)
);