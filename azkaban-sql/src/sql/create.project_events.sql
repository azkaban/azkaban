CREATE TABLE if NOT EXISTS project_events (
	project_id INT NOT NULL,
	event_type TINYINT NOT NULL,
	event_time BIGINT NOT NULL,
	username VARCHAR(64),
	message VARCHAR(512)
)
ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE INDEX log ON project_events(project_id, event_time);
