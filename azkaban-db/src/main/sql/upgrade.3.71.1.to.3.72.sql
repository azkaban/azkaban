-- DB Migration from 3.71.1 to 3.72

-- PR #???? changes the type of project_flows.json from BLOB to LONGBLOB
ALTER TABLE project_flows ALTER COLUMN json LONGBLOB;
