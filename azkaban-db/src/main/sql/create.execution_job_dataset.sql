CREATE TABLE execution_job_dataset (
   id               INT(11)         NOT NULL AUTO_INCREMENT,
   exec_id          INT(11)         NOT NULL,
   job_id           VARCHAR(512)    NOT NULL,
   attempt          INT(11)         NOT NULL,
   dataset_type     VARCHAR(16)     NOT NULL,
   raw_dataset      VARCHAR(512)    NOT NULL,
   resolved_dataset VARCHAR(512)    NOT NULL,
   PRIMARY KEY (id)
);

CREATE INDEX ex_id
    ON execution_job_dataset (exec_id);

CREATE UNIQUE INDEX unique_idx
    ON execution_job_dataset(exec_id, job_id, attempt, dataset_type, raw_dataset);
