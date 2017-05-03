CREATE TABLE if NOT EXISTS project_properties (
	project_id INT NOT NULL,
	version INT NOT NULL,
	name VARCHAR(255),
	modified_time BIGINT NOT NULL,
	encoding_type TINYINT,
	property BLOB,
	PRIMARY KEY (project_id, version, name)
)
ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE INDEX properties_index ON project_properties(project_id, version);
