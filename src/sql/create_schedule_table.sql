DROP TABLE if exists schedules;

CREATE TABLE schedules (
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
	enc_type TINYINT,
    	schedule_options LONGBLOB,
	primary key(project_id, flow_name)
) ENGINE=InnoDB;

