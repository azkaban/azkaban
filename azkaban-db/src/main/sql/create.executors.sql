CREATE TABLE executors (
  id     INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
  host   VARCHAR(64) NOT NULL,
  port   INT         NOT NULL,
  active BOOLEAN                          DEFAULT FALSE,
  UNIQUE (host, port),
  UNIQUE INDEX executor_id (id)
);

CREATE INDEX executor_connection
  ON executors (host, port);

CREATE TABLE executor_tags (
  executor_id INT         NOT NULL REFERENCES executors (id),
  tag         VARCHAR(64) NOT NULL,
  PRIMARY KEY (executor_id, tag)
);
