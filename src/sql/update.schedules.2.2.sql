ALTER TABLE schedules DROP PRIMARY KEY;
ALTER TABLE schedules ADD COLUMN schedule_id INT PRIMARY KEY NOT NULL AUTO_INCREMENT;
ALTER TABLE schedules ADD INDEX schedule_project_id (project_id, flow_name);
