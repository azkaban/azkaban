-- DB Migration from release 3.80.1 to 3.81.0
-- PR #2363 Implements thin archive support. The thin archive json text must be stored as a blob along with the
-- project version.
--
ALTER TABLE project_versions ADD startup_dependencies MEDIUMBLOB DEFAULT NULL;
