-- In table execution_logs, name is the combination of flow_id and job_id
--
-- prefix support and lengths of prefixes (where supported) are storage engine dependent.
-- By default, the index key prefix length limit is 767 bytes for innoDB.
-- from: https://dev.mysql.com/doc/refman/5.7/en/create-index.html

CREATE TABLE execution_logs (
  exec_id     INT NOT NULL,
  name        VARCHAR(640),
  attempt     INT,
  enc_type    TINYINT,
  start_byte  INT,
  end_byte    INT,
  log         LONGBLOB,
  upload_time BIGINT,
  PRIMARY KEY (exec_id, name, attempt, start_byte)
);

CREATE INDEX ex_log_attempt
  ON execution_logs (exec_id, name, attempt);
CREATE INDEX ex_log_index
  ON execution_logs (exec_id, name);
CREATE INDEX ex_log_upload_time
  ON execution_logs (upload_time);
