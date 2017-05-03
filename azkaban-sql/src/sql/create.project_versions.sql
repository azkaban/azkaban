CREATE TABLE if NOT EXISTS project_versions (
	project_id INT NOT NULL,
	version INT not NULL,
	upload_time BIGINT NOT NULL,
	uploader VARCHAR(64) NOT NULL,
	file_type VARCHAR(16),
	file_name VARCHAR(128),
	md5 BINARY(16),
	num_chunks INT,
	uri VARCHAR(512) DEFAULT NULL,
	PRIMARY KEY (project_id, version)
)
ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE INDEX version_index ON project_versions(project_id);
