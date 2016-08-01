CREATE TABLE active_sla (
	exec_id INT NOT NULL,
	job_name VARCHAR(128) NOT NULL,
	check_time BIGINT NOT NULL,
	rule SMALLINT NOT NULL,
	enc_type SMALLINT,
	options BYTEA NOT NULL,
	primary key(exec_id, job_name)
);