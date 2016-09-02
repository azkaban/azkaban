CREATE TABLE schedules (
	schedule_id SERIAL NOT NULL PRIMARY KEY,
	project_id INT NOT NULL,
	project_name VARCHAR(128) NOT NULL,
	flow_name VARCHAR(128) NOT NULL,
	status VARCHAR(16),
	first_sched_time BIGINT,
	timezone VARCHAR(64),
	period VARCHAR(16),
	last_modify_time BIGINT,
	next_exec_time BIGINT,
	submit_time BIGINT,
	submit_user VARCHAR(128),
	enc_type SMALLINT,
	schedule_options BYTEA
);

CREATE INDEX sched_project_id ON schedules(project_id, flow_name);
