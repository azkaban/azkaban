-- DB Migration from release 3.20.0 to 3.21.0
--
-- PR #1024 introduces a new column to 'project_versions' table.
--
ALTER TABLE project_versions ADD uri VARCHAR(512);
