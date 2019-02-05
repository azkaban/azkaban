-- DB Migration from release 3.68.0 to 3.69.0
-- PR #xxxx Implement "useExecutor" feature for new dispatching Logic (Poll model).
-- use_executor column contains the id of the executor that should handle the execution.
-- This id is a parameter an Azkaban admin can specify when launching a new execution.
--
ALTER TABLE execution_flows ADD COLUMN use_executor INT DEFAULT -1;
