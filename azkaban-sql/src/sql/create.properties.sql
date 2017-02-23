CREATE TABLE if NOT EXISTS properties (
	name VARCHAR(64) NOT NULL,
	type INT NOT NULL,
	modified_time BIGINT NOT NULL,
	value VARCHAR(256),
	PRIMARY KEY (name, type)
)
ENGINE=InnoDB DEFAULT CHARSET=latin1;
