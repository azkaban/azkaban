CREATE TABLE active_executing_flows (
	exec_id INT,
	host VARCHAR(255),
	port INT,
	update_time BIGINT,
	PRIMARY KEY (exec_id)
);