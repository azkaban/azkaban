-- DB Migration from release 4.10.0 to 4.11.0
-- Increase the size of the column recording project events.
ALTER TABLE project_events MODIFY message TEXT;
