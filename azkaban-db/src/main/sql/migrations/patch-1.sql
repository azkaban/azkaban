-- DB Migration from release 3.83.0
-- Increasing the size of json column in project_flows table.
ALTER TABLE project_flows MODIFY COLUMN json MEDIUMBLOB;
