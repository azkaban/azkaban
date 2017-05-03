CREATE TABLE if NOT EXISTS executors (
  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  host VARCHAR(64) NOT NULL,
  port INT NOT NULL,
  active BOOLEAN DEFAULT false,
  UNIQUE (host, port),
  UNIQUE INDEX executor_id (id)
)
ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE INDEX executor_connection ON executors(host, port);
