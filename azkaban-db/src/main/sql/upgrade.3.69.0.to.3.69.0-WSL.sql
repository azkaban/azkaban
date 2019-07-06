-- TODO DB Migration from release 3.69.0 to 3.69.0-WSL
-- PR #???? Implement "executor tags" feature for new dispatching Logic (Poll model).
--
ALTER TABLE executors ADD enc_type TINYINT DEFAULT 1;
ALTER TABLE executors ADD executor_data LONGBLOB DEFAULT NULL;
