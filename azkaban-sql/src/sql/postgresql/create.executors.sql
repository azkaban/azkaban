CREATE TABLE executors (
  id SERIAL NOT NULL PRIMARY KEY,
  host VARCHAR(64) NOT NULL,
  port INT NOT NULL,
  active BOOLEAN DEFAULT true
);
CREATE UNIQUE INDEX executor_id
   ON executors (id ASC NULLS LAST);
CREATE INDEX executor_connection ON executors(host, port);
ALTER TABLE executors
  ADD CONSTRAINT executor_uc UNIQUE (host, port);

