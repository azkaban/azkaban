-- DB Migration from release 4.10.0 to 4.11.0
-- Increase the size of the column recording project event details.
ALTER TABLE project_events MODIFY message VARCHAR(16000);
