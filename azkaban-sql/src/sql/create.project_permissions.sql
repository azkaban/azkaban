CREATE TABLE if NOT EXISTS project_permissions (
	project_id VARCHAR(64) NOT NULL,
	modified_time BIGINT NOT NULL,
	name VARCHAR(64) NOT NULL,
	permissions INT NOT NULL,
	isGroup BOOLEAN NOT NULL,
	PRIMARY KEY (project_id, name)
)
ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE INDEX permission_index ON project_permissions(project_id);
