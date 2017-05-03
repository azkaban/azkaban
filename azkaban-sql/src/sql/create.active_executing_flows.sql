CREATE TABLE if NOT EXISTS active_executing_flows (
	exec_id INT,
	update_time BIGINT,
	PRIMARY KEY (exec_id)
)
ENGINE=InnoDB DEFAULT CHARSET=latin1;
