-- DB Migration from release 3.20.0 to 3.22.0
--
-- Release 3.21.0 is broken
-- PR #1024 introduces a new column to 'project_versions' table.
--
ALTER TABLE project_versions
  ADD resource_id VARCHAR(512);
