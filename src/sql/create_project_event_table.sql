CREATE TABLE project_events (
	project_id INT NOT NULL,
	event_type TINYINT NOT NULL,
	event_time BIGINT NOT NULL,
	username VARCHAR(64),
	message VARCHAR(512),
	INDEX log (project_id, event_time)
) ENGINE=InnoDB;
