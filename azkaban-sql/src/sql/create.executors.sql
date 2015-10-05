CREATE TABLE executors (
  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  host VARCHAR(64) NOT NULL,
  port INT NOT NULL,
  active BOOLEAN DEFAULT true,
  UNIQUE (host, port),
  UNIQUE INDEX executor_id (id)
);

CREATE INDEX executor_connection ON executors(host, port);
