CREATE TABLE properties (
	name VARCHAR(64) NOT NULL,
	modified_time BIGINT NOT NULL,
	property BLOB,
	PRIMARY KEY (name)
) ENGINE=InnoDB;

INSERT INTO properties (name, modified_time, property) VALUES ( UNIX_TIMESTAMP(), "2.1")