CREATE TABLE project_versions (
	project_id INT NOT NULL,
	version INT not NULL,
	upload_time BIGINT NOT NULL,
	uploader VARCHAR(64) NOT NULL,
	file_type VARCHAR(16),
	file_name VARCHAR(128),
	md5 BINARY(16),
	num_chunks INT,
	PRIMARY KEY (project_id, version)
);

CREATE INDEX version_index ON project_versions(project_id);
