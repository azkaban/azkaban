-- DB Migration from release 3.85.0 to 3.86.0
-- Adding ExecutionSource column in execution_flows
ALTER TABLE execution_flows ADD COLUMN execution_source VARCHAR(32) DEFAULT NULL;
