/*
For 2.01 Adds the attempt column to execution_jobs
*/
ALTER TABLE execution_jobs ADD COLUMN attempt INT DEFAULT 0;
ALTER TABLE execution_jobs DROP PRIMARY KEY;
ALTER TABLE execution_jobs ADD PRIMARY KEY(exec_id, job_id, attempt);
ALTER TABLE execution_jobs ADD INDEX exec_job (exec_id, job_id);

ALTER TABLE execution_logs ADD COLUMN attempt INT DEFAULT 0;
ALTER TABLE execution_logs ADD COLUMN upload_time BIGINT DEFAULT 1420099200000;
UPDATE execution_logs SET upload_time=(UNIX_TIMESTAMP()*1000) WHERE upload_time=1420099200000;

ALTER TABLE execution_logs DROP PRIMARY KEY;
ALTER TABLE execution_logs ADD PRIMARY KEY(exec_id, name, attempt, start_byte);
ALTER TABLE execution_logs ADD INDEX log_attempt (exec_id, name, attempt);

ALTER TABLE schedules ADD COLUMN enc_type TINYINT;
ALTER TABLE schedules ADD COLUMN schedule_options LONGBLOB;

ALTER TABLE schedules DROP PRIMARY KEY;
ALTER TABLE schedules ADD COLUMN schedule_id INT PRIMARY KEY NOT NULL AUTO_INCREMENT;
ALTER TABLE schedules ADD INDEX project_id (project_id, flow_name);

ALTER TABLE project_events MODIFY COLUMN message VARCHAR(512);

ALTER TABLE projects ADD COLUMN enc_type TINYINT;
ALTER TABLE projects ADD COLUMN settings_blob LONGBLOB;

CREATE TABLE active_sla (
	exec_id INT NOT NULL,
	job_name VARCHAR(128) NOT NULL,
	check_time BIGINT NOT NULL,
	rule TINYINT NOT NULL,
	enc_type TINYINT,
	options LONGBLOB NOT NULL,
	primary key(exec_id, job_name)
) ENGINE=InnoDB;
