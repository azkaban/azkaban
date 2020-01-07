-- DB Migration from release 3.76.0 to 3.77.0
-- PR #2311 Fixes issue of group permissions not being saved when group name matches an existing
-- user's name.
--
ALTER TABLE project_permissions DROP PRIMARY KEY, ADD PRIMARY KEY (project_id, name, isGroup);
