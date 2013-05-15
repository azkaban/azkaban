ALTER TABLE execution_jobs ADD COLUMN attempt INT DEFAULT 0;
ALTER TABLE execution_jobs DROP PRIMARY KEY;
ALTER TABLE execution_jobs ADD PRIMARY KEY(exec_id, job_id, attempt);
ALTER TABLE execution_jobs ADD INDEX exec_job (exec_id, job_id);
