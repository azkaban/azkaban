CREATE TABLE active_executing_flows (
	exec_id INT,
	host VARCHAR(255),
	port INT,
	update_time BIGINT,
	PRIMARY KEY (exec_id)
);

ALTER TABLE active_executing_flows DROP COLUMN host;
ALTER TABLE active_executing_flows DROP COLUMN port;