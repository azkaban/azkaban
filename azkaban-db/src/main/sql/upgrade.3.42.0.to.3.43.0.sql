-- DB Migration from release 3.42.0 to 3.43.0
-- PR #1657 changes project cache to case insensitive.
--
-- 1. When creating new database,
-- use below query to explicitly set the COLLATION to case insensitive for the entire database:
--
ALTER DATABASE <database_name> CHARACTER SET utf8 COLLATE utf8_general_ci;
--
-- 2. For existing database,
-- use below query to explicitly set the COLLATION to case insensitive for "name" column in
-- "projects" table:
--
ALTER TABLE projects MODIFY name VARCHAR(64) CHARACTER SET utf8 COLLATE utf8_general_ci;
