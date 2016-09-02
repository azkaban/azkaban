CREATE TABLE project_files (
	project_id INT NOT NULL,
	version INT not NULL,
	chunk INT,
	size INT,
	file BYTEA,
	PRIMARY KEY (project_id, version, chunk)
);

CREATE INDEX file_version ON project_files(project_id, version);