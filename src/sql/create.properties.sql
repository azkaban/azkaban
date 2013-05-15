CREATE TABLE properties (
	name VARCHAR(64) NOT NULL,
	type INT NOT NULL,
	modified_time BIGINT NOT NULL,
	value VARCHAR(256),
	PRIMARY KEY (id, type)
);