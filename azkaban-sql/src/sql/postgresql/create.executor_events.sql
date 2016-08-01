CREATE TABLE executor_events (
  executor_id INT NOT NULL,
  event_type SMALLINT NOT NULL,
  event_time TIMESTAMP NOT NULL,
  username VARCHAR(64),
  message VARCHAR(512)
);

CREATE INDEX executor_log ON executor_events(executor_id, event_time);