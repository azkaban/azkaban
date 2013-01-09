CREATE TABLE project_files (
	project_id INT NOT NULL,
	version INT not NULL,
	chunk INT,
	size INT,
	file LONGBLOB,
	PRIMARY KEY (project_id, version, chunk),
	INDEX file_version (project_id, version)
) ENGINE=InnoDB;
