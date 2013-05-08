---
layout: documents
nav: upgradefrom20
context: ../..
---

# Upgrading DB from 2.0

If installing Azkaban from scratch, you can ignore this document. This is only for those who are upgrading from 2.0 to 2.1.

The _update\_2.0\_to\_2.1.sql_ needs to be run to alter all the tables. This includes several table alterations and a new table.

Here are the changes:

* Alter execution_jobs table
	* add int column 'attempt'
	* change primary key to (exec_id, job_id, attempt)
	* add index for (exec_id, job_id)
* Alter execution_logs table
	* add int column 'attempt'
	* add bigint column 'upload_time'
	* default upload_time to current time in millisec
	* change primary key to (exec_id, name, attempt, start_byte)
	* add index for (exec_id, name, attempt)
* Alter schedules table
	* Add tinyint column 'enc_type'
	* Add longblob column 'schedule_options'
* Alter project_events table
	* Modify 'message' column to be 512 characters
* Alter projects table
	* add tinyint column enc_type
	* add longblob column projects
* Create new table active_sla
