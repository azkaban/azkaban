CREATE TABLE executors (
  id     INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
  host   VARCHAR(64) NOT NULL,
  port   INT         NOT NULL,
  active BOOLEAN                          DEFAULT FALSE,
  enc_type TINYINT                        DEFAULT 1,
  executor_data LONGBLOB                  DEFAULT NULL,
  UNIQUE (host, port)
);

CREATE INDEX executor_connection
  ON executors (host, port);
