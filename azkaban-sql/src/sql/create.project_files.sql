CREATE TABLE if NOT EXISTS project_files (
	project_id INT NOT NULL,
	version INT not NULL,
	chunk INT,
	size INT,
	file LONGBLOB,
	PRIMARY KEY (project_id, version, chunk)
)
ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE INDEX file_version ON project_files(project_id, version);
