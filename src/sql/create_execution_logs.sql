CREATE TABLE execution_logs (
	exec_id INT NOT NULL,
	name VARCHAR(128),
	enc_type TINYINT,
	start_byte INT,
	end_byte INT,
	log LONGBLOB,
	INDEX log_index (exec_id, name),
	INDEX byte_log_index(exec_id, name, start_byte, end_byte)
) ENGINE=InnoDB;
