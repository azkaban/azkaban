CREATE TABLE triggers (
	trigger_id INT NOT NULL AUTO_INCREMENT,
	modify_time BIGINT NOT NULL,
	enc_type TINYINT,
	data LONGBLOB,
	PRIMARY KEY (trigger_id)
);
